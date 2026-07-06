#include "l3_l4_interop_server.h"

#include "l3_l4_interop.capnp.h"

#include <capnp/message.h>
#include <capnp/rpc.h>
#include <capnp/serialize-async.h>
#include <kj/async-queue.h>
#include <kj/debug.h>
#include <kj/map.h>

#include <cstdint>
#include <cstring>

#if CAPNP_VERSION >= 2000000

namespace {

using namespace e2e::l3l4;

using InteropVatNetworkBase =
    capnp::VatNetwork<VatId, ThirdPartyCompletion, ThirdPartyToAwait, ThirdPartyToContact, JoinResult>;

constexpr uint32_t NUMBER_VALUE = 4242;

enum class ScenarioMode: uint16_t {
  HAPPY = 0,
  BAD_CONTACT_FALLBACK = 1,
  MALFORMED_COMPLETION = 2,
  REJECTED_COMPLETION = 3,
  REJECT_AWAIT = 4,
  DROP_AFTER_PROVIDE = 5,
  DUPLICATE_ACCEPT = 6,
  NUMBER_EXCEPTION = 7,
};

class InteropVatNetwork final : public InteropVatNetworkBase {
public:
  using Connection = InteropVatNetworkBase::Connection;

  InteropVatNetwork() = default;

  uint64_t newToken() {
    return ++tokenCounter;
  }

  void addAccepted(kj::Own<kj::AsyncIoStream>&& stream) {
    acceptQueue.push(kj::Own<Connection>(
        kj::refcounted<ConnectionImpl>(*this, kj::mv(stream), "zig")));
  }

  kj::Maybe<kj::Own<Connection>> connect(VatId::Reader hostId) override {
    if (hostId.getHost() == "cpp-host") {
      return kj::none;
    }
    return kj::none;
  }

  kj::Promise<kj::Own<Connection>> accept() override {
    return acceptQueue.pop();
  }

private:
  friend class ConnectionImpl;
  friend class NumberImpl;

  struct ThirdPartyExchange {
    kj::ForkedPromise<kj::Rc<kj::Refcounted>> promise;
    kj::Own<kj::PromiseFulfiller<kj::Rc<kj::Refcounted>>> fulfiller;
    bool completed = false;

    ThirdPartyExchange(kj::PromiseFulfillerPair<kj::Rc<kj::Refcounted>> paf =
                       kj::newPromiseAndFulfiller<kj::Rc<kj::Refcounted>>())
        : promise(paf.promise.fork()), fulfiller(kj::mv(paf.fulfiller)) {}
  };

  static ScenarioMode modeFromToken(uint64_t token) {
    return static_cast<ScenarioMode>(token >> 48);
  }

  static kj::Promise<kj::Rc<kj::Refcounted>> rejectedThirdParty(kj::StringPtr reason) {
    return kj::Promise<kj::Rc<kj::Refcounted>>(KJ_EXCEPTION(FAILED, reason));
  }

  ThirdPartyExchange& getExchange(uint64_t token) {
    return exchanges.findOrCreate(token, [&]() -> decltype(exchanges)::Entry {
      return {token, ThirdPartyExchange()};
    });
  }

  class IncomingMessageImpl final : public capnp::IncomingRpcMessage {
  public:
    explicit IncomingMessageImpl(kj::Own<capnp::MessageReader> message)
        : message(kj::mv(message)) {}

    capnp::AnyPointer::Reader getBody() override {
      return message->getRoot<capnp::AnyPointer>();
    }

    size_t sizeInWords() override {
      return message->sizeInWords();
    }

  private:
    kj::Own<capnp::MessageReader> message;
  };

  class ConnectionImpl final : public Connection, public kj::Refcounted {
  public:
    ConnectionImpl(InteropVatNetwork& network, kj::Own<kj::AsyncIoStream>&& stream, kj::StringPtr peerName)
        : network(network),
          peerName(peerName),
          ioStream(kj::mv(stream)),
          messageStream(kj::heap<capnp::BufferedMessageStream>(
              *ioStream, capnp::IncomingRpcMessage::getShortLivedCallback())),
          peerVatId(4),
          previousWrite(kj::READY_NOW) {
      peerVatId.initRoot<VatId>().setHost(peerName);
    }

    VatId::Reader getPeerVatId() override {
      return peerVatId.getRoot<VatId>();
    }

    kj::Own<capnp::OutgoingRpcMessage> newOutgoingMessage(unsigned int firstSegmentWordSize) override;

    kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> receiveIncomingMessage() override {
      return kj::evalLater([this]() -> kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> {
        KJ_IF_SOME(e, readCancelReason) {
          return kj::cp(e);
        }
        kj::Array<kj::OwnFd> fdSpace = nullptr;
        auto promise = readCanceler.wrap(messageStream->tryReadMessage(fdSpace, receiveOptions));
        return promise.then([](kj::Maybe<capnp::MessageReaderAndFds>&& messageAndFds)
                                -> kj::Maybe<kj::Own<capnp::IncomingRpcMessage>> {
          KJ_IF_SOME(m, messageAndFds) {
            return kj::Own<capnp::IncomingRpcMessage>(
                kj::heap<IncomingMessageImpl>(kj::mv(m.reader)));
          }
          return kj::none;
        });
      });
    }

    kj::Promise<void> shutdown() override {
      KJ_IF_SOME(write, previousWrite) {
        auto result = write.then([this]() {
          return messageStream->end();
        });
        previousWrite = kj::none;
        return kj::mv(result);
      }
      return kj::READY_NOW;
    }

    void setIdle(bool value) override {
      idle = value;
    }

    bool canIntroduceTo(Connection& other) override {
      return true;
    }

    void introduceTo(Connection& other,
        ThirdPartyToContact::Builder otherContactInfo,
        ThirdPartyToAwait::Builder thisAwaitInfo) override {
      uint64_t token = network.newToken();
      otherContactInfo.setHost(kj::downcast<ConnectionImpl>(other).peerName);
      otherContactInfo.setToken(token);
      otherContactInfo.setSentBy("cpp-host");
      thisAwaitInfo.setToken(token);
    }

    kj::Maybe<kj::Own<Connection>> connectToIntroduced(
        ThirdPartyToContact::Reader contact,
        ThirdPartyCompletion::Builder completion) override {
      completion.setToken(contact.getToken());
      return kj::none;
    }

    kj::Own<void> awaitThirdParty(
        ThirdPartyToAwait::Reader party,
        kj::Rc<kj::Refcounted> value) override {
      uint64_t token = party.getToken();
      auto mode = InteropVatNetwork::modeFromToken(token);
      if (mode == ScenarioMode::REJECT_AWAIT) {
        auto& exchange = network.getExchange(token);
        (void)exchange;
        return kj::heap(kj::defer([this, token]() {
          network.exchanges.erase(token);
        }));
      }

      auto& exchange = network.getExchange(token);
      exchange.fulfiller->fulfill(kj::mv(value));
      if (mode == ScenarioMode::DROP_AFTER_PROVIDE) {
        ioStream->shutdownWrite();
      }

      return kj::heap(kj::defer([this, token]() {
        network.exchanges.erase(token);
      }));
    }

    kj::Promise<kj::Rc<kj::Refcounted>> completeThirdParty(
        ThirdPartyCompletion::Reader completion) override {
      uint64_t token = completion.getToken();
      auto mode = InteropVatNetwork::modeFromToken(token);
      if (token == 0 || mode == ScenarioMode::MALFORMED_COMPLETION ||
          mode == ScenarioMode::REJECTED_COMPLETION ||
          mode == ScenarioMode::REJECT_AWAIT) {
        return InteropVatNetwork::rejectedThirdParty("l3_l4_interop rejected third-party completion");
      }
      KJ_IF_SOME(completed, network.completedTokens.find(token)) {
        (void)completed;
        return InteropVatNetwork::rejectedThirdParty("l3_l4_interop duplicate third-party completion");
      }

      auto& exchange = network.getExchange(token);
      if (exchange.completed) {
        return InteropVatNetwork::rejectedThirdParty("l3_l4_interop duplicate third-party completion");
      }
      exchange.completed = true;
      network.completedTokens.findOrCreate(token, [&]() -> decltype(network.completedTokens)::Entry {
        return {token, true};
      });
      if (mode == ScenarioMode::NUMBER_EXCEPTION) {
        network.failNextNumberCall += 1;
      }
      return exchange.promise.addBranch();
    }

    kj::Array<kj::byte> generateEmbargoId() override {
      auto token = network.newToken();
      auto result = kj::heapArray<kj::byte>(sizeof(token));
      std::memcpy(result.begin(), &token, sizeof(token));
      return result;
    }

  private:
    class OutgoingMessageImpl final : public capnp::OutgoingRpcMessage, public kj::Refcounted {
    public:
      OutgoingMessageImpl(ConnectionImpl& connection, unsigned int firstSegmentWordSize)
          : connection(connection),
            message(firstSegmentWordSize == 0 ? capnp::SUGGESTED_FIRST_SEGMENT_WORDS
                                              : firstSegmentWordSize) {}

      capnp::AnyPointer::Builder getBody() override {
        return message.getRoot<capnp::AnyPointer>();
      }

      void send() override {
        size_t size = 0;
        for (auto& segment: message.getSegmentsForOutput()) {
          size += segment.size();
        }
        KJ_REQUIRE(size < connection.receiveOptions.traversalLimitInWords, size,
            "message exceeds traversal limit") {
          return;
        }

        KJ_IF_SOME(write, connection.previousWrite) {
          auto selfRef = addRefToThis();
          connection.previousWrite = write.then(
              [&connection = connection, selfRef = kj::mv(selfRef)]() mutable {
            return connection.messageStream->writeMessage(selfRef->message.getSegmentsForOutput())
                .attach(kj::mv(selfRef));
          }).catch_([&connection = connection](kj::Exception&& e) {
            connection.readCancelReason = kj::cp(e);
            if (!connection.readCanceler.isEmpty()) {
              connection.readCanceler.cancel(kj::cp(e));
            }
            kj::throwRecoverableException(kj::mv(e));
          }).eagerlyEvaluate(nullptr);
        }
      }

      size_t sizeInWords() override {
        return message.sizeInWords();
      }

    private:
      ConnectionImpl& connection;
      capnp::MallocMessageBuilder message;
    };

    InteropVatNetwork& network;
    kj::StringPtr peerName;
    kj::Own<kj::AsyncIoStream> ioStream;
    kj::Own<capnp::MessageStream> messageStream;
    capnp::MallocMessageBuilder peerVatId;
    capnp::ReaderOptions receiveOptions;
    kj::Maybe<kj::Promise<void>> previousWrite;
    kj::Canceler readCanceler;
    kj::Maybe<kj::Exception> readCancelReason;
    bool idle = true;
  };

  kj::ProducerConsumerQueue<kj::Own<Connection>> acceptQueue;
  kj::HashMap<uint64_t, ThirdPartyExchange> exchanges;
  kj::HashMap<uint64_t, bool> completedTokens;
  uint64_t tokenCounter = 0;
  uint32_t failNextNumberCall = 0;
};

kj::Own<capnp::OutgoingRpcMessage> InteropVatNetwork::ConnectionImpl::newOutgoingMessage(
    unsigned int firstSegmentWordSize) {
  return kj::Own<capnp::OutgoingRpcMessage>(
      kj::refcounted<OutgoingMessageImpl>(*this, firstSegmentWordSize));
}

class NumberImpl final : public Number::Server {
public:
  explicit NumberImpl(InteropVatNetwork& network): network(network) {}

  kj::Promise<void> getNumber(GetNumberContext context) override {
    if (network.failNextNumberCall > 0) {
      network.failNextNumberCall -= 1;
      return kj::Promise<void>(KJ_EXCEPTION(FAILED, "l3_l4_interop forced Number.getNumber failure"));
    }
    context.getResults().setN(NUMBER_VALUE);
    return kj::READY_NOW;
  }

private:
  InteropVatNetwork& network;
};

kj::Promise<void> acceptLoop(kj::ConnectionReceiver& listener, InteropVatNetwork& vatNetwork) {
  return listener.accept().then([&listener, &vatNetwork](kj::Own<kj::AsyncIoStream>&& connection) {
    vatNetwork.addAccepted(kj::mv(connection));
    return acceptLoop(listener, vatNetwork);
  });
}

}  // namespace

void runL3L4InteropServer(kj::ConnectionReceiver& listener, kj::WaitScope& waitScope) {
  InteropVatNetwork vatNetwork;
  Number::Client bootstrap = kj::heap<NumberImpl>(vatNetwork);
  auto rpcSystem = capnp::makeRpcServer(vatNetwork, kj::mv(bootstrap));
  auto acceptTask = acceptLoop(listener, vatNetwork).eagerlyEvaluate([](kj::Exception&& exception) {
    KJ_LOG(ERROR, exception);
  });
  acceptTask.wait(waitScope);
}

#else

void runL3L4InteropServer(kj::ConnectionReceiver& listener, kj::WaitScope& waitScope) {
  (void)listener;
  (void)waitScope;
  KJ_FAIL_REQUIRE("l3_l4_interop requires Cap'n Proto C++ >= 2.0.0 for generic VatNetwork Level-3 hooks");
}

#endif
