// The C++ half of the fd-passing e2e (rpc_unix_fd_cpp_test.zig, sprint
// item 15): the reference's "send FD over RPC" and "FD per message limit"
// tests (c++/src/capnp/rpc-twoparty-test.c++), with capnp-zig on the other
// end of the connection, in both directions.
//
// For each case this driver makes an AF_UNIX socketpair, forks, and execs
// the Zig endpoint (rpc_unix_fd_cpp_endpoint.zig) with one end. It then
// runs the C++ side on the other end: TwoPartyClient(stream, maxFds) or
// TwoPartyServer::accept(stream, maxFds). Linux only: there the kernel
// gives a message's fds with its first byte, which kj relies on.
//
// Usage: driver <path to the Zig endpoint>

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include "rpc_fd.capnp.h"

namespace {

// The fill byte at `i`; the Zig endpoint writes and checks the same one.
uint8_t fillByte(size_t i) { return static_cast<uint8_t>(i % 251); }

void writeAll(int fd, const char* text) {
  size_t len = strlen(text);
  size_t off = 0;
  while (off < len) {
    ssize_t n;
    KJ_SYSCALL(n = ::write(fd, text + off, len - off));
    off += static_cast<size_t>(n);
  }
}

struct Pipe {
  kj::AutoCloseFd in;
  kj::AutoCloseFd out;
};

Pipe makePipe() {
  int fds[2];
  KJ_SYSCALL(::pipe(fds));
  return Pipe{kj::AutoCloseFd(fds[0]), kj::AutoCloseFd(fds[1])};
}

// Everything `fd` gives until EOF: every copy of the pipe's write end, in
// both processes, must be closed first.
kj::String readAll(kj::AsyncIoContext& io, int fd) {
  return io.lowLevelProvider->wrapInputFd(fd)->readAllText().wait(io.waitScope);
}

// The reference's TestFdCap: a capability that owns an fd.
class FdCapImpl final: public FdCap::Server {
public:
  explicit FdCapImpl(kj::AutoCloseFd fd): fd(kj::mv(fd)) {}
  kj::Maybe<int> getFd() override { return fd.get(); }

private:
  kj::AutoCloseFd fd;
};

// The reference's TestMoreStuffImpl::writeToFd, plus a check of each fill.
class FdTestImpl final: public FdTest::Server {
public:
  explicit FdTestImpl(std::vector<uint32_t> fills): fills(kj::mv(fills)) {}
  size_t calls = 0;

  kj::Promise<void> writeToFd(WriteToFdContext context) override {
    auto params = context.getParams();
    KJ_REQUIRE(calls < fills.size(), "more calls than fills", calls);
    auto fill = params.getFill();
    KJ_REQUIRE(fill.size() == fills[calls], "fill size", fill.size(), fills[calls]);
    for (size_t i = 0; i < fill.size(); ++i) {
      KJ_REQUIRE(fill[i] == fillByte(i), "fill corrupted", i);
    }
    ++calls;

    auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
    promises.add(params.getFdCap1().getFd().then([](kj::Maybe<int> fd) {
      writeAll(KJ_ASSERT_NONNULL(fd), "foo");
    }));
    promises.add(params.getFdCap2().getFd().then([context](kj::Maybe<int> fd) mutable {
      int raw = fd.orDefault(-1);
      context.getResults().setSecondFdPresent(raw >= 0);
      if (raw >= 0) writeAll(raw, "bar");
    }));

    auto pipe = makePipe();
    {
      kj::AutoCloseFd out = kj::mv(pipe.out);
      writeAll(out.get(), "baz");
    }
    context.getResults().setFdCap3(kj::heap<FdCapImpl>(kj::mv(pipe.in)));

    return kj::joinPromises(promises.finish());
  }

private:
  std::vector<uint32_t> fills;
};

// The reference tests' client side: per fill size, two pipes whose write
// ends ride on fdCap1 and fdCap2 (reversed), one call, then every pipe is
// read to EOF while the connection is up.
void runCppClient(kj::AsyncIoContext& io, kj::AsyncCapabilityStream& stream, uint maxFds,
                  bool expectSecond, const std::vector<uint32_t>& fills) {
  capnp::TwoPartyClient client(stream, maxFds);
  auto cap = client.bootstrap().castAs<FdTest>();

  for (uint32_t fillSize: fills) {
    auto pipe1 = makePipe();
    auto pipe2 = makePipe();

    capnp::RemotePromise<FdTest::WriteToFdResults> promise = nullptr;
    {
      auto req = cap.writeToFdRequest();
      auto fill = req.initFill(fillSize);
      for (uint32_t i = 0; i < fillSize; ++i) fill.set(i, fillByte(i));
      // Order reversal intentional, as in the reference.
      req.setFdCap1(kj::heap<FdCapImpl>(kj::mv(pipe2.out)));
      req.setFdCap2(kj::heap<FdCapImpl>(kj::mv(pipe1.out)));
      promise = req.send();
    }

    // The fd on a pipelined capability: getFd() waits for the Return.
    int in3 = KJ_ASSERT_NONNULL(promise.getFdCap3().getFd().wait(io.waitScope));
    auto baz = readAll(io, in3);
    KJ_REQUIRE(baz == "baz", baz, fillSize);

    {
      auto promise2 = kj::mv(promise);  // make sure the PipelineHook also goes out of scope
      auto response = promise2.wait(io.waitScope);
      KJ_REQUIRE(response.getSecondFdPresent() == expectSecond, "secondFdPresent", expectSecond,
                 maxFds, fillSize);
    }

    auto bar = readAll(io, pipe1.in.get());
    KJ_REQUIRE(bar == (expectSecond ? "bar" : ""), bar, fillSize);
    auto foo = readAll(io, pipe2.in.get());
    KJ_REQUIRE(foo == "foo", foo, fillSize);
  }
}

void runCppServer(kj::AsyncIoContext& io, kj::AsyncCapabilityStream& stream, uint maxFds,
                  const std::vector<uint32_t>& fills) {
  auto impl = kj::heap<FdTestImpl>(fills);
  auto& ref = *impl;
  capnp::TwoPartyServer server(kj::mv(impl));
  // Resolves when the Zig client closes the connection.
  server.accept(stream, maxFds).wait(io.waitScope);
  KJ_REQUIRE(ref.calls == fills.size(), "calls served", ref.calls, fills.size());
}

void run(const char* endpoint, bool cppClient, uint maxFds, bool expectSecond,
         std::vector<uint32_t> fills) {
  std::cout << (cppClient ? "C++ client -> Zig server" : "Zig client -> C++ server")
            << ", " << maxFds << " fd(s) per message, " << fills.size() << " call(s)" << std::endl;
  int sockets[2];
  KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets));

  std::vector<std::string> args = {
      endpoint, std::to_string(sockets[1]), cppClient ? "server" : "client",
      std::to_string(maxFds), expectSecond ? "1" : "0"};
  for (uint32_t fill: fills) args.push_back(std::to_string(fill));
  std::vector<char*> argv;
  for (auto& arg: args) argv.push_back(arg.data());
  argv.push_back(nullptr);

  auto child = fork();
  KJ_REQUIRE(child >= 0);
  if (child == 0) {
    close(sockets[0]);
    execv(endpoint, argv.data());
    _exit(127);
  }
  close(sockets[1]);
  {
    auto io = kj::setupAsyncIo();
    auto stream = io.lowLevelProvider->wrapUnixSocketFd(
        sockets[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    if (cppClient) {
      runCppClient(io, *stream, maxFds, expectSecond, fills);
    } else {
      runCppServer(io, *stream, maxFds, fills);
    }
  }
  int status;
  KJ_SYSCALL(waitpid(child, &status, 0));
  KJ_REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Zig endpoint failed", status,
             cppClient, maxFds);
}

}  // namespace

int main(int argc, char** argv) {
  KJ_REQUIRE(argc == 2);
  signal(SIGPIPE, SIG_IGN);
  alarm(120);
  try {
    for (bool cppClient: {true, false}) {
      // "send FD over RPC": up to 2 fds per message on both ends, fills that
      // spread one message over many reads.
      run(argv[1], cppClient, 2, true, {1024 * 1024, 65536, 8192, 0});
      // "FD per message limit": 1 fd per message, so fdCap2 arrives without one.
      run(argv[1], cppClient, 1, false, {0});
    }
    std::cout << "C++ <-> Zig fd passing over AF_UNIX passed" << std::endl;
  } catch (const kj::Exception& e) {
    std::cerr << e.getDescription().cStr() << std::endl;
    return 1;
  }
}
