#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
#include "streaming.capnp.h"

struct SlowServer final: TestStreaming::Server {
  kj::Timer& timer;
  unsigned total = 0;
  unsigned completed = 0;
  bool barrier = false;
  explicit SlowServer(kj::Timer& timer): timer(timer) {}
  kj::Promise<void> doStreamI(DoStreamIContext context) override {
    auto value = context.getParams().getI();
    return timer.afterDelay(20 * kj::MILLISECONDS).then([this, value]() {
      KJ_REQUIRE(value == completed + 1, "stream handlers completed out of order");
      ++completed;
      total += value;
    });
  }
  kj::Promise<void> doStreamJ(DoStreamJContext) override { return kj::READY_NOW; }
  kj::Promise<void> finishStream(FinishStreamContext context) override {
    KJ_REQUIRE(completed == 2, "barrier overtook deferred streaming work");
    context.getResults().setTotalI(total);
    barrier = true;
    return kj::READY_NOW;
  }
};

static void run(const char* endpoint, bool cppClient) {
  int sockets[2];
  KJ_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
  auto child = fork();
  KJ_REQUIRE(child >= 0);
  if (child == 0) {
    close(sockets[0]);
    auto fd = std::to_string(sockets[1]);
    execl(endpoint, endpoint, fd.c_str(), cppClient ? "server" : "client", nullptr);
    _exit(127);
  }
  close(sockets[1]);
  {
    auto io = kj::setupAsyncIo();
    auto stream = io.lowLevelProvider->wrapSocketFd(sockets[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    if (cppClient) {
      capnp::TwoPartyClient rpc(*stream);
      auto client = rpc.bootstrap().castAs<TestStreaming>();
      auto first = client.doStreamIRequest();
      first.setI(1);
      auto sentFirst = first.send();
      auto second = client.doStreamIRequest();
      second.setI(2);
      auto sentSecond = second.send();
      auto done = client.finishStreamRequest().send().wait(io.waitScope);
      KJ_REQUIRE(done.getTotalI() == 3, "Zig lost ordered streaming values");
      sentFirst.wait(io.waitScope);
      sentSecond.wait(io.waitScope);
    } else {
      auto implementation = kj::heap<SlowServer>(io.provider->getTimer());
      auto& implementationRef = *implementation;
      capnp::TwoPartyServer rpc(kj::mv(implementation));
      rpc.accept(*stream).wait(io.waitScope);
      KJ_REQUIRE(implementationRef.barrier && implementationRef.completed == 2);
    }
  }
  int status;
  KJ_REQUIRE(waitpid(child, &status, 0) == child);
  KJ_REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Zig endpoint failed", status);
}
int main(int argc, char** argv) {
  KJ_REQUIRE(argc == 2);
  signal(SIGPIPE, SIG_IGN);
  alarm(30);
  try {
    run(argv[1], true);
    run(argv[1], false);
    std::cout << "C++ <-> Zig deferred streaming, bounded calls, readiness, and barriers passed\n";
    return 0;
  } catch (const kj::Exception& e) {
    std::cerr << e.getDescription().cStr() << "\n";
    return 1;
  }
}
