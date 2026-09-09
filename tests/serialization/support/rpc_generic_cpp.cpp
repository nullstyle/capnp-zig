#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include "generic_rpc.capnp.h"
#include "generic_rpc_external.capnp.h"

template <typename T> struct EchoServer final: Service<T>::Server {
  bool done = false;
  kj::Promise<void> echo(typename Service<T>::Server::EchoContext context) override {
    context.getResults().setValue(context.getParams().getValue());
    done = true;
    return kj::READY_NOW;
  }
};
struct InheritedServer final: TextChild::Server {
  bool done = false;
  kj::Promise<void> echo(EchoContext context) override {
    KJ_REQUIRE(context.getParams().getValue() == "generic interop");
    context.getResults().setValue(context.getParams().getValue());
    done = true;
    return kj::READY_NOW;
  }
};
struct MethodServer final: Factory::Server {
  bool done = false;
  kj::Promise<void> identity(IdentityContext context) override {
    context.getResults().setValue(context.getParams().getValue());
    done = true;
    return kj::READY_NOW;
  }
};
struct PipelineLeaf final: Service<capnp::Text>::Server {
  bool& done;
  explicit PipelineLeaf(bool& done): done(done) {}
  kj::Promise<void> echo(EchoContext context) override {
    KJ_REQUIRE(context.getParams().getValue() == "pipelined generic");
    context.getResults().setValue(context.getParams().getValue());
    done = true;
    return kj::READY_NOW;
  }
};
struct PipelineServer final: Factory::Server {
  kj::Timer& timer;
  bool done = false;
  explicit PipelineServer(kj::Timer& timer): timer(timer) {}
  kj::Promise<void> getService(GetServiceContext context) override {
    return timer.afterDelay(20 * kj::MILLISECONDS).then([this, context = kj::mv(context)]() mutable {
      context.getResults().initBox().setValue(kj::heap<PipelineLeaf>(done));
    });
  }
};
static const kj::byte bytes[] = {0xff, 0, 7};
static void checkData(capnp::Data::Reader data) {
  KJ_REQUIRE(data.size() == 3 && data[0] == 0xff && data[1] == 0 && data[2] == 7);
}
static void call(capnp::TwoPartyClient& rpc, kj::WaitScope& wait, std::string binding) {
  if (binding == "text") {
    auto request = rpc.bootstrap().castAs<Service<capnp::Text>>().echoRequest();
    request.setValue("generic interop");
    KJ_REQUIRE(request.send().wait(wait).getValue() == "generic interop");
  } else if (binding == "data") {
    auto request = rpc.bootstrap().castAs<Service<capnp::Data>>().echoRequest();
    request.setValue(kj::arrayPtr(bytes, 3));
    checkData(request.send().wait(wait).getValue());
  } else if (binding == "inherited") {
    auto request = rpc.bootstrap().castAs<TextChild>().echoRequest();
    request.setValue("generic interop");
    KJ_REQUIRE(request.send().wait(wait).getValue() == "generic interop");
  } else if (binding == "pipeline") {
    auto pending = rpc.bootstrap().castAs<Factory>().getServiceRequest().send();
    auto echo = pending.getBox().getValue().echoRequest();
    echo.setValue("pipelined generic");
    KJ_REQUIRE(echo.send().wait(wait).getValue() == "pipelined generic");
    pending.wait(wait);
  } else {
    auto request = rpc.bootstrap().castAs<Factory>().identityRequest<capnp::Data>();
    request.setValue(kj::arrayPtr(bytes, 3));
    checkData(request.send().wait(wait).getValue());
  }
}
template <typename Server> static void serve(kj::AsyncIoStream& stream, kj::WaitScope& wait) {
  auto implementation = kj::heap<Server>();
  auto& ref = *implementation;
  capnp::TwoPartyServer rpc(kj::mv(implementation));
  rpc.accept(stream).wait(wait);
  KJ_REQUIRE(ref.done);
}
static void run(const char* endpoint, bool cppClient, const char* binding) {
  int sockets[2];
  KJ_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
  auto child = fork(); KJ_REQUIRE(child >= 0);
  if (child == 0) {
    close(sockets[0]); auto fd = std::to_string(sockets[1]);
    execl(endpoint, endpoint, fd.c_str(), cppClient ? "server" : "client", binding, nullptr);
    _exit(127);
  }
  close(sockets[1]);
  {
    auto io = kj::setupAsyncIo();
    auto stream = io.lowLevelProvider->wrapSocketFd(sockets[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    if (cppClient) { capnp::TwoPartyClient rpc(*stream); call(rpc, io.waitScope, binding); }
    else if (std::string(binding) == "text") serve<EchoServer<capnp::Text>>(*stream, io.waitScope);
    else if (std::string(binding) == "data") serve<EchoServer<capnp::Data>>(*stream, io.waitScope);
    else if (std::string(binding) == "inherited") serve<InheritedServer>(*stream, io.waitScope);
    else if (std::string(binding) == "pipeline") {
      auto implementation = kj::heap<PipelineServer>(io.provider->getTimer());
      auto& ref = *implementation;
      capnp::TwoPartyServer rpc(kj::mv(implementation));
      rpc.accept(*stream).wait(io.waitScope);
      KJ_REQUIRE(ref.done);
    } else serve<MethodServer>(*stream, io.waitScope);
  }
  int status; KJ_REQUIRE(waitpid(child, &status, 0) == child);
  KJ_REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) == 0, "Zig endpoint failed", status);
}
int main(int argc, char** argv) {
  KJ_REQUIRE(argc == 2); signal(SIGPIPE, SIG_IGN); alarm(30);
  try {
    for (auto binding: {"text", "data", "inherited", "method", "pipeline"}) { run(argv[1], true, binding); run(argv[1], false, binding); }
    std::cout << "C++ <-> Zig generic interfaces, inherited bindings, and method generics passed\n";
  } catch (const kj::Exception& e) { std::cerr << e.getDescription().cStr() << "\n"; return 1; }
}
