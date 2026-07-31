// C++ driver for the cross-impl VatC hosting harness: this ONE process plays
// vat A (recipient) and vat B (introducer) against a capnp-zig two-peer VatC
// host reached over real TCP.
//
// Architecture (modeled on the vendored rpc-test.c++ TestVat fixture):
//   - One kj event loop, one process.
//   - One custom VatNetwork *endpoint instance per vat* (TestVat pattern). The
//     hard C++ constraint (rpc.c++:1730-1737) is that the introducer B's two
//     legs — B<->A and B<->C — are connections of the same RpcSystem/network,
//     which holds here because both are legs of vat B's single endpoint.
//   - The A<->B link is in-process (message-queue connection pair, TestVat
//     pattern). The B->C and A->C links are real TCP dials to the zig host
//     using standard capnp framing (BufferedMessageStream, the same
//     ConnectionImpl shape as l3_l4_interop_server.cpp).
//
// Level-3 hooks:
//   - canIntroduceTo: true iff the introduced cap lives on the TCP link to the
//     zig host (the only introductions this harness supports).
//   - introduceTo: mints ThirdPartyToContact{host = the "<host>:<port>" argv
//     address, token = counter, sentBy = introducing vat's name} and
//     ThirdPartyToAwait{token}.
//   - connectToIntroduced: returns this vat's pre-dialed TCP connection to the
//     host and fills ThirdPartyCompletion{token} (token+1 under the
//     unknown-token scenario).
//   - generateEmbargoId: fresh 8-byte counter (the default throws).
//
// Scenarios (argv: <host> <port> <scenario>):
//   happy         B imports Carol from the host via the returner bootstrap,
//                 introduces her to A; A calls the accepted cap (forcing the
//                 lazy Accept) and asserts 42.
//   embargo       A pipelines a call on the still-resolving promise
//                 (rpc-test.c++:2272 shape) so the Accept goes out immediately
//                 WITH an embargo; both the pipelined and a direct
//                 post-resolution call must return 42.
//   unknown-token completion token corrupted (+1); the Accept must PARK (the
//                 rendezvous is order-independent, rpc.h:483-492) rather than
//                 fail, and must not wedge either leg to the host.
//   disconnect    happy, then both host connections are dropped abruptly (no
//                 clean shutdown) via _exit(0).
//   pipelined-provide, pipelined-provide-chain
//                 B introduces a still-PIPELINED result cap that re-resolves
//                 to B's own local cap; the host must refuse the Accept
//                 fail-closed (CrossPeerReceiverHostedTargetUnsupported).
//                 See the scenario branch in runDriver for the two shapes.
//
// Output: TAP on stdout ("ok N - ..." + "1..N"); exit 0 iff all asserts pass.

#include "l3_l4_interop.capnp.h"

#include <capnp/any.h>
#include <capnp/common.h>
#include <capnp/message.h>
#include <capnp/rpc.h>
#include <capnp/serialize.h>
#include <capnp/serialize-async.h>
#include <kj/async-io.h>
#include <kj/async-queue.h>
#include <kj/debug.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

#include <unistd.h>

#if CAPNP_VERSION >= 2000000

namespace {

using namespace e2e::l3l4;

// Returner-bootstrap call convention, mirroring the vatc unit test's
// grantCapVia (tests/rpc/peer/rpc_three_party_handoff_vatc_test.zig:387-395):
// one call on the host's bootstrap whose Return payload carries Carol as a
// capability (content root, or pointer field 0 of a result struct — both
// shapes are accepted below).
constexpr uint64_t RETURNER_INTERFACE_ID = 0xC0C0C0C0C0C0C001ull;
constexpr uint16_t RETURNER_METHOD_ID = 0;

// The A<->B holder interface is internal to this process: B's bootstrap is a
// raw Capability::Server whose only method returns the held (imported) Carol
// at the result content root.
constexpr uint64_t HOLDER_INTERFACE_ID = 0x6C337663686F6C64ull;  // "l3vchold"
constexpr uint16_t HOLDER_METHOD_ID = 0;

struct Tap {
  int n = 0;
  bool failed = false;

  void ok(bool cond, const char* desc) {
    ++n;
    printf("%s %d - %s\n", cond ? "ok" : "not ok", n, desc);
    fflush(stdout);
    if (!cond) failed = true;
  }

  void plan() {
    printf("1..%d\n", n);
    fflush(stdout);
  }
};

using DriverVatBase = capnp::VatNetwork<
    VatId, ThirdPartyCompletion, ThirdPartyToAwait, ThirdPartyToContact, JoinResult>;

class DriverVat;

struct DriverNetwork {
  // Shared cross-vat state (the TestNetwork analogue).
  kj::StringPtr hostAddr;  // "<host>:<port>" exactly as dialed; goes into ThirdPartyToContact.host
  bool corruptCompletionToken = false;
  uint64_t tokenCounter = 0;
  uint64_t embargoCounter = 0;

  uint64_t newToken() { return ++tokenCounter; }
};

class DriverConnectionBase: public DriverVatBase::Connection {
  // Common Level-3 hook implementations shared by the in-process and TCP
  // connection classes. All connections of this network are
  // DriverConnectionBase instances, per the VatNetwork contract that hook
  // arguments are connections of the same network.
public:
  explicit DriverConnectionBase(DriverVat& vat): vat(vat) {}

  virtual bool isHostLink() = 0;
  // True for the TCP connection to the zig host, false for the in-process
  // A<->B link.

  bool canIntroduceTo(Connection& other) override;
  void introduceTo(Connection& other,
      ThirdPartyToContact::Builder otherContactInfo,
      ThirdPartyToAwait::Builder thisAwaitInfo) override;
  kj::Maybe<kj::Own<Connection>> connectToIntroduced(
      ThirdPartyToContact::Reader contact,
      ThirdPartyCompletion::Builder completion) override;
  kj::Array<kj::byte> generateEmbargoId() override;

protected:
  DriverVat& vat;
};

class LocalConnectionImpl final: public DriverConnectionBase, public kj::Refcounted {
  // In-process connection half (TestVat ConnectionImpl pattern, minus the
  // test-only block/idle machinery): outgoing messages are flattened and
  // pushed synchronously onto the partner's queue, preserving send order.
public:
  LocalConnectionImpl(DriverVat& vat, kj::StringPtr peerName)
      : DriverConnectionBase(vat), peerVatId(4) {
    peerVatId.initRoot<VatId>().setHost(peerName);
  }

  ~LocalConnectionImpl() noexcept(false) {
    if (partner != nullptr) {
      partner->partner = nullptr;
    }
  }

  void attach(LocalConnectionImpl& other) {
    KJ_REQUIRE(partner == nullptr && other.partner == nullptr);
    partner = &other;
    other.partner = this;
  }

  bool isHostLink() override { return false; }

  VatId::Reader getPeerVatId() override { return peerVatId.getRoot<VatId>(); }

  class IncomingMessageImpl final: public capnp::IncomingRpcMessage {
  public:
    explicit IncomingMessageImpl(kj::Array<capnp::word> data)
        : data(kj::mv(data)), message(this->data) {}

    capnp::AnyPointer::Reader getBody() override {
      return message.getRoot<capnp::AnyPointer>();
    }

    size_t sizeInWords() override { return data.size(); }

  private:
    kj::Array<capnp::word> data;
    capnp::FlatArrayMessageReader message;
  };

  class OutgoingMessageImpl final: public capnp::OutgoingRpcMessage {
  public:
    OutgoingMessageImpl(LocalConnectionImpl& connection, unsigned int firstSegmentWordSize)
        : connection(connection),
          message(firstSegmentWordSize == 0 ? capnp::SUGGESTED_FIRST_SEGMENT_WORDS
                                            : firstSegmentWordSize) {}

    capnp::AnyPointer::Builder getBody() override {
      return message.getRoot<capnp::AnyPointer>();
    }

    void send() override {
      if (connection.partner == nullptr) return;
      connection.partner->messageQueue.push(kj::Own<capnp::IncomingRpcMessage>(
          kj::heap<IncomingMessageImpl>(capnp::messageToFlatArray(message))));
    }

    size_t sizeInWords() override { return message.sizeInWords(); }

  private:
    LocalConnectionImpl& connection;
    capnp::MallocMessageBuilder message;
  };

  kj::Own<capnp::OutgoingRpcMessage> newOutgoingMessage(unsigned int firstSegmentWordSize) override {
    return kj::heap<OutgoingMessageImpl>(*this, firstSegmentWordSize);
  }

  kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> receiveIncomingMessage() override {
    return messageQueue.pop();
  }

  kj::Promise<void> shutdown() override {
    if (partner != nullptr) {
      partner->messageQueue.push(kj::none);
    }
    return kj::READY_NOW;
  }

  void setIdle(bool value) override { idle = value; }

private:
  LocalConnectionImpl* partner = nullptr;
  kj::ProducerConsumerQueue<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> messageQueue;
  capnp::MallocMessageBuilder peerVatId;
  bool idle = true;
};

class TcpConnectionImpl final: public DriverConnectionBase, public kj::Refcounted {
  // TCP leg to the zig host: standard capnp framing over a kj::AsyncIoStream
  // (same MessageStream shape as l3_l4_interop_server.cpp's ConnectionImpl /
  // rpc-twoparty).
public:
  TcpConnectionImpl(DriverVat& vat, kj::Own<kj::AsyncIoStream>&& stream)
      : DriverConnectionBase(vat),
        ioStream(kj::mv(stream)),
        messageStream(kj::heap<capnp::BufferedMessageStream>(
            *ioStream, capnp::IncomingRpcMessage::getShortLivedCallback())),
        peerVatId(4),
        previousWrite(kj::READY_NOW) {
    peerVatId.initRoot<VatId>().setHost("zig-host");
  }

  bool isHostLink() override { return true; }

  VatId::Reader getPeerVatId() override { return peerVatId.getRoot<VatId>(); }

  kj::Own<capnp::OutgoingRpcMessage> newOutgoingMessage(unsigned int firstSegmentWordSize) override;

  kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> receiveIncomingMessage() override {
    return kj::evalLater([this]() -> kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> {
      KJ_IF_SOME(e, readCancelReason) {
        return e.clone();
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

  void setIdle(bool value) override { idle = value; }

private:
  class IncomingMessageImpl final: public capnp::IncomingRpcMessage {
  public:
    explicit IncomingMessageImpl(kj::Own<capnp::MessageReader> message)
        : message(kj::mv(message)) {}

    capnp::AnyPointer::Reader getBody() override {
      return message->getRoot<capnp::AnyPointer>();
    }

    size_t sizeInWords() override { return message->sizeInWords(); }

  private:
    kj::Own<capnp::MessageReader> message;
  };

  class OutgoingMessageImpl final: public capnp::OutgoingRpcMessage, public kj::Refcounted {
  public:
    OutgoingMessageImpl(TcpConnectionImpl& connection, unsigned int firstSegmentWordSize)
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
          connection.readCancelReason = e.clone();
          if (!connection.readCanceler.isEmpty()) {
            connection.readCanceler.cancel(e.clone());
          }
          kj::throwRecoverableException(kj::mv(e));
        }).eagerlyEvaluate(nullptr);
      }
    }

    size_t sizeInWords() override { return message.sizeInWords(); }

  private:
    TcpConnectionImpl& connection;
    capnp::MallocMessageBuilder message;
  };

  kj::Own<kj::AsyncIoStream> ioStream;
  kj::Own<capnp::MessageStream> messageStream;
  capnp::MallocMessageBuilder peerVatId;
  capnp::ReaderOptions receiveOptions;
  kj::Maybe<kj::Promise<void>> previousWrite;
  kj::Canceler readCanceler;
  kj::Maybe<kj::Exception> readCancelReason;
  bool idle = true;
};

kj::Own<capnp::OutgoingRpcMessage> TcpConnectionImpl::newOutgoingMessage(
    unsigned int firstSegmentWordSize) {
  return kj::Own<capnp::OutgoingRpcMessage>(
      kj::refcounted<OutgoingMessageImpl>(*this, firstSegmentWordSize));
}

class DriverVat final: public DriverVatBase {
  // One vat endpoint (the TestVat analogue). Vat B holds the TCP leg to the
  // host plus the accepted half of the in-process A<->B pair; vat A holds its
  // own TCP leg plus the initiating half.
public:
  DriverVat(DriverNetwork& network, kj::StringPtr self): network(network), self(self) {}

  DriverNetwork& network;
  kj::StringPtr self;
  DriverVat* partner = nullptr;

  void adoptHostStream(kj::Own<kj::AsyncIoStream>&& stream) {
    auto conn = kj::refcounted<TcpConnectionImpl>(*this, kj::mv(stream));
    hostConn = conn.get();
    hostConnOwn = kj::Own<Connection>(kj::mv(conn));
  }

  kj::Own<Connection> hostConnectionRef() {
    KJ_REQUIRE(hostConn != nullptr, "no host connection established for this vat", self);
    return kj::Own<Connection>(kj::addRef(*hostConn));
  }

  kj::Maybe<kj::Own<Connection>> connect(VatId::Reader hostId) override {
    auto host = hostId.getHost();
    if (host == network.hostAddr) {
      return hostConnectionRef();
    }
    if (partner != nullptr && host == partner->self) {
      if (localConn != nullptr) {
        return kj::Own<Connection>(kj::addRef(*localConn));
      }
      auto local = kj::refcounted<LocalConnectionImpl>(*this, partner->self);
      auto remote = kj::refcounted<LocalConnectionImpl>(*partner, self);
      local->attach(*remote);
      localConn = local.get();
      partner->localConn = remote.get();
      partner->acceptQueue.push(kj::Own<Connection>(kj::mv(remote)));
      return kj::Own<Connection>(kj::mv(local));
    }
    KJ_FAIL_REQUIRE("unknown vat id in connect()", host);
  }

  kj::Promise<kj::Own<Connection>> accept() override { return acceptQueue.pop(); }

private:
  // Raw pointers are weak conveniences; ownership lives in hostConnOwn (one
  // process-lifetime ref) and in the RpcSystems. Scenarios are one-shot, so
  // no dangling access happens after teardown begins.
  TcpConnectionImpl* hostConn = nullptr;
  kj::Maybe<kj::Own<Connection>> hostConnOwn;
  LocalConnectionImpl* localConn = nullptr;
  kj::ProducerConsumerQueue<kj::Own<Connection>> acceptQueue;
};

bool DriverConnectionBase::canIntroduceTo(Connection& other) {
  return kj::downcast<DriverConnectionBase>(other).isHostLink();
}

void DriverConnectionBase::introduceTo(Connection& other,
    ThirdPartyToContact::Builder otherContactInfo,
    ThirdPartyToAwait::Builder thisAwaitInfo) {
  KJ_REQUIRE(kj::downcast<DriverConnectionBase>(other).isHostLink(),
      "harness only introduces capabilities hosted on the zig host");
  uint64_t token = vat.network.newToken();
  otherContactInfo.setHost(vat.network.hostAddr);
  otherContactInfo.setToken(token);
  otherContactInfo.setSentBy(vat.self);
  thisAwaitInfo.setToken(token);
}

kj::Maybe<kj::Own<DriverVatBase::Connection>> DriverConnectionBase::connectToIntroduced(
    ThirdPartyToContact::Reader contact,
    ThirdPartyCompletion::Builder completion) {
  KJ_REQUIRE(contact.getHost() == vat.network.hostAddr,
      "third-party contact does not name the zig host", contact.getHost());
  uint64_t token = contact.getToken();
  if (vat.network.corruptCompletionToken) {
    token += 1;  // unknown-token scenario: rendezvous bytes will not match
  }
  completion.setToken(token);
  return vat.hostConnectionRef();
}

kj::Array<kj::byte> DriverConnectionBase::generateEmbargoId() {
  uint64_t id = ++vat.network.embargoCounter;
  auto result = kj::heapArray<kj::byte>(sizeof(id));
  memcpy(result.begin(), &id, sizeof(id));
  return result;
}

class HolderImpl final: public capnp::Capability::Server {
  // Vat B's bootstrap toward A: one typeless method returning the held
  // (host-imported) Carol at the result content root. Returning a cap that B
  // imported over B<->C into a message on B<->A is exactly the writeDescriptor
  // trigger for the automatic Provide + thirdPartyHosted emission.
public:
  capnp::Capability::Client held = nullptr;

  DispatchCallResult dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    KJ_REQUIRE(interfaceId == HOLDER_INTERFACE_ID && methodId == HOLDER_METHOD_ID,
        "unexpected call on holder bootstrap", interfaceId, methodId);
    context.getResults(capnp::MessageSize {4, 1}).setAs<capnp::Capability>(held);
    return { kj::READY_NOW, false, true };
  }
};

class DeadEndImpl final: public capnp::Capability::Server {
  // B's local capability for the pipelined-provide scenarios. It is the cap
  // the host ends up re-resolving the provided pipeline to (an IMPORT from
  // the host's perspective). The Zig host must fail the Accept closed, so
  // this must never actually be called.
public:
  int calls = 0;

  DispatchCallResult dispatchCall(uint64_t, uint16_t,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>) override {
    ++calls;
    KJ_FAIL_REQUIRE("spike dead-end capability must never be invoked");
  }
};

capnp::Capability::Client readRootCap(capnp::AnyPointer::Reader root) {
  // Accept both returner result shapes: the vatc-test CapReturner puts the
  // capability at the payload content root; a schema-typed returner would put
  // it in pointer field 0 of a result struct.
  if (root.isCapability()) {
    return root.getAs<capnp::Capability>();
  }
  if (root.isStruct()) {
    auto ptrs = root.getAs<capnp::AnyStruct>().getPointerSection();
    KJ_REQUIRE(ptrs.size() > 0, "returner result struct has no pointer fields");
    return ptrs[0].getAs<capnp::Capability>();
  }
  KJ_FAIL_REQUIRE("returner result carries no capability");
}

int runDriver(kj::StringPtr host, kj::StringPtr port, kj::StringPtr scenario) {
  Tap tap;
  auto hostAddr = kj::str(host, ":", port);

  auto io = kj::setupAsyncIo();
  auto& ws = io.waitScope;

  // Pre-dial both legs to the zig host (it accepts exactly two connections;
  // order is not significant to it). Pre-dialing keeps connectToIntroduced
  // synchronous, matching the fixed-lookup pattern of the existing lane.
  auto addr = io.provider->getNetwork().parseAddress(hostAddr).wait(ws);
  auto streamB = addr->connect().wait(ws);
  auto streamA = addr->connect().wait(ws);

  DriverNetwork shared;
  shared.hostAddr = hostAddr;
  shared.corruptCompletionToken = (scenario == "unknown-token");

  DriverVat vatA(shared, "a");
  DriverVat vatB(shared, "b");
  vatA.partner = &vatB;
  vatB.partner = &vatA;
  vatB.adoptHostStream(kj::mv(streamB));
  vatA.adoptHostStream(kj::mv(streamA));

  auto holderOwn = kj::heap<HolderImpl>();
  HolderImpl& holder = *holderOwn;
  auto rpcB = capnp::makeRpcServer(vatB, capnp::Capability::Client(kj::mv(holderOwn)));
  auto rpcA = capnp::makeRpcClient(vatA);

  // B: bootstrap the host's returner and import Carol wire-honestly (she is
  // Return-carried, not the bootstrap).
  capnp::MallocMessageBuilder hostIdMsg(8);
  auto hostId = hostIdMsg.initRoot<VatId>();
  hostId.setHost(hostAddr);
  auto returner = rpcB.bootstrap(hostId.asReader());

  if (scenario == "pipelined-provide" || scenario == "pipelined-provide-chain") {
    // B introduces a still-PIPELINED (unresolved) result cap to A. The
    // host's bootstrap in these scenarios ECHOES its single param cap, so
    // the pipelined result ultimately re-resolves to B's own local cap — a
    // cap the host only IMPORTS. rpc.c++ writes the Provide target via
    // PipelineClient::writeTarget => promisedAnswer{questionId, ops}; no
    // shortening is possible while the question is outstanding.
    //
    //   pipelined-provide        held = pipeline of Q2 = echo(bCap). The
    //                            host resolves the promisedAnswer target at
    //                            Provide time to the already-answered Q2's
    //                            receiverHosted cap (fail-closed site 1).
    //   pipelined-provide-chain  held = pipeline of Q1 = echo(pipeline of
    //                            Q2); the Q1 answer carries a chained
    //                            receiverAnswer{Q2} descriptor, so the host
    //                            stores .promised ops and only re-resolves
    //                            at ACCEPT time (fail-closed site 2, the
    //                            .promised -> .imported arm).
    //
    // Both refusals are CURRENT fail-closed behavior: the wire-visible
    // outcome A must observe is the Accept answered with an exception naming
    // CrossPeerReceiverHostedTargetUnsupported. Rewrite these asserts when
    // the receiverHosted lift lands.
    auto deadEndOwn = kj::heap<DeadEndImpl>();
    DeadEndImpl& deadEnd = *deadEndOwn;
    capnp::Capability::Client bCap(kj::mv(deadEndOwn));

    // Q2: echo(bCap) — sent, NOT awaited. Keep q2p alive so no Finish is
    // sent (the host's recorded answer must survive until the Accept).
    auto q2 = returner.typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {});
    q2.setAs<capnp::Capability>(bCap);
    auto q2p = q2.send();

    kj::Maybe<capnp::RemotePromise<capnp::AnyPointer>> keepQ1;
    if (scenario == "pipelined-provide-chain") {
      // Q1: echo(pipelined result of Q2) — the param descriptor B writes for
      // a same-connection PipelineClient is receiverAnswer{Q2}.
      auto q1 = returner.typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {});
      q1.setAs<capnp::Capability>(capnp::Capability::Client(q2p.asCap()));
      auto q1p = q1.send();
      holder.held = capnp::Capability::Client(q1p.asCap());
      keepQ1 = kj::mv(q1p);
    } else {
      holder.held = capnp::Capability::Client(q2p.asCap());
    }
    tap.ok(true, "B holds a still-pipelined (unresolved) result cap");

    // A: fetch the held cap from B; B's answer forces the third-party
    // introduction of the PIPELINED cap (thirdPartyHosted + Provide with a
    // promisedAnswer target on the B leg).
    capnp::MallocMessageBuilder bIdMsgSpike(8);
    auto bIdSpike = bIdMsgSpike.initRoot<VatId>();
    bIdSpike.setHost("b");
    auto holderCapSpike = rpcA.bootstrap(bIdSpike.asReader());
    auto resp = holderCapSpike
        .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
        .send().wait(ws);
    Number::Client accepted = readRootCap(resp).castAs<Number>();
    tap.ok(true, "A received the introduced third-party cap from B");

    // First use forces the lazy Accept; the host must refuse it fail-closed.
    // Observe the Accept's own Return via whenResolved() (the unknown-token
    // pattern) rather than a pipelined call: a call pipelined on the refused
    // Accept question is a separate surface (empirically the host never
    // answers it — a known gap), while whenResolved() settles on the Accept
    // exception itself.
    bool threw = false;
    bool named = false;
    kj::String desc = kj::str("<no exception>");
    KJ_IF_SOME(e, kj::runCatchingExceptions([&]() {
      accepted.whenResolved().wait(ws);
    })) {
      threw = true;
      desc = kj::str(e.getDescription());
      named = strstr(desc.cStr(), "CrossPeerReceiverHostedTargetUnsupported") != nullptr;
    }
    printf("# accept outcome: %s\n", desc.cStr());
    fflush(stdout);
    tap.ok(threw, "accepting the pipelined-provided cap failed (Accept refused)");
    tap.ok(named, "the Accept exception names CrossPeerReceiverHostedTargetUnsupported");
    tap.ok(deadEnd.calls == 0, "B's local dead-end cap was never invoked");

    tap.plan();
    return tap.failed ? 1 : 0;
  }

  auto returnerResp = returner
      .typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {})
      .send().wait(ws);
  holder.held = readRootCap(returnerResp);
  tap.ok(true, "B imported Carol from the zig host via the returner bootstrap");

  // A: bootstrap B's holder over the in-process leg.
  capnp::MallocMessageBuilder bIdMsg(8);
  auto bId = bIdMsg.initRoot<VatId>();
  bId.setHost("b");
  auto holderCap = rpcA.bootstrap(bId.asReader());

  if (scenario == "embargo") {
    // Pipelined-resolve shape (rpc-test.c++:2272): pipeline a call on the
    // still-unresolved holder result so the promise resolves to the
    // thirdPartyHosted cap with calls outstanding -> immediate Accept WITH
    // embargo, released by B's forwarded promisedAnswer-form Disembargo.
    auto promise = holderCap
        .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
        .send();
    Number::Client pipelined(promise.asCap());
    auto call1 = pipelined.getNumberRequest().send();

    auto resp = promise.wait(ws);
    Number::Client direct = readRootCap(resp).castAs<Number>();
    auto call2 = direct.getNumberRequest().send();

    uint32_t n1 = call1.wait(ws).getN();
    tap.ok(n1 == 42, "pipelined (embargo-forcing) call returned 42");
    uint32_t n2 = call2.wait(ws).getN();
    tap.ok(n2 == 42, "post-resolution direct call returned 42");
    // generateEmbargoId is only invoked for embargoed accepts (rpc.c++:2224),
    // so this proves the Accept actually went out WITH an embargo.
    tap.ok(shared.embargoCounter > 0, "recipient generated an embargo id (Accept was embargoed)");
  } else {
    // The Response is scoped so the ONLY surviving ref to the accepted cap is
    // `carol`; the happy path below drops it to force a real wire `Release`.
    Number::Client carol(nullptr);
    {
      auto resp = holderCap
          .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      carol = readRootCap(resp).castAs<Number>();
    }
    tap.ok(true, "A received the introduced third-party cap from B");

    if (scenario == "unknown-token") {
      // WHAT THIS CELL CAN AND CANNOT PROVE.
      //
      // An Accept whose ThirdPartyCompletion matches no Provide must PARK, not
      // fail. The rendezvous is order-independent by contract — rpc.h:483-492:
      // "The promise resolves when some other connection on this VatNetwork
      // calls `awaitThirdParty()` ... The two calls can happen in any order;
      // `completeThirdParty()` will wait for a corresponding
      // `awaitThirdParty()` if it hasn't happened already." The reference C++
      // implements exactly that: TestVat::completeThirdParty `findOrCreate`s a
      // ThirdPartyExchange and returns its forked promise (rpc-test.c++:534-537
      // + :639-643), so an unmatched token yields a promise nothing ever
      // fulfills. rpc.c++'s handleAccept then simply never sends a Return.
      //
      // So "the host answers an unmatched token with an 'unknown provision'
      // exception" is NOT the correct expectation and is NOT observable from
      // this driver — asserting it would be asserting a spec violation. What
      // IS provable, and what this cell now asserts:
      //   (1) the Accept parks: no Return arrives within a generous window;
      //   (2) parking one Accept does not wedge either connection to the host.
      // The host binary asserts the state side (parked_accept_count went up,
      // no proxy export minted, everything drains at disconnect).
      constexpr unsigned kParkWindowSeconds = 3;
      auto& timer = io.provider->getTimer();
      bool settled = carol.whenResolved()
          .then([]() { return true; }, [](kj::Exception&&) { return true; })
          .exclusiveJoin(timer.afterDelay(kParkWindowSeconds * kj::SECONDS)
              .then([]() { return false; }))
          .wait(ws);
      tap.ok(!settled,
          "accept with an unmatched completion token parks (no Return within the window)");

      // Liveness, both legs. The introducer leg (B<->C) still serves calls on
      // the ordinary Carol import...
      uint32_t viaB = holder.held.castAs<Number>().getNumberRequest().send().wait(ws).getN();
      tap.ok(viaB == 42, "introducer leg still serves calls while the Accept is parked");

      // ...and the recipient leg (A<->C), the one holding the parked Accept
      // question, still answers new questions.
      auto returnerA = rpcA.bootstrap(hostId.asReader());
      auto respA = returnerA
          .typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      (void)respA;
      tap.ok(true, "recipient leg still answers new questions while its Accept is parked");
    } else {
      // happy / disconnect: first use forces the lazy Accept.
      uint32_t n = carol.getNumberRequest().send().wait(ws).getN();
      tap.ok(n == 42, "accepted cap call returned 42");
      tap.ok(shared.embargoCounter == 0, "plain handoff sent a non-embargoed Accept");

      if (scenario == "disconnect") {
        tap.ok(true, "dropping both host connections abruptly (no clean shutdown)");
        tap.plan();
        fflush(stdout);
        fflush(stderr);
        // _exit: no destructors, no RPC release/finish ceremony — the kernel
        // closes both TCP sockets out from under the host.
        _exit(tap.failed ? 1 : 0);
      }

      // happy: perform the RPC-level RELEASE ceremony that `disconnect`
      // deliberately skips. Dropping the last client ref to A's import of the
      // accepted capability makes the C++ RpcSystem emit `Release` on the A
      // leg; the host must then destroy the cross-peer proxy export and drop
      // its source-side back-link. Returning from main would NOT do this: the
      // kj event loop never turns again after the scope ends, so the queued
      // Release would die with the process. Turn the loop explicitly.
      carol = nullptr;
      io.provider->getTimer().afterDelay(250 * kj::MILLISECONDS).wait(ws);
      tap.ok(true, "released the accepted cap and flushed the Release to the host");
    }
  }

  tap.plan();
  return tap.failed ? 1 : 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 4) {
    fprintf(stderr, "usage: l3_vatc_client <host> <port> <scenario>\n"
                    "  scenario: happy | embargo | unknown-token | disconnect |\n"
                    "            pipelined-provide | pipelined-provide-chain\n");
    return 2;
  }
  kj::StringPtr host = argv[1];
  kj::StringPtr port = argv[2];
  kj::StringPtr scenario = argv[3];

  if (scenario != "happy" && scenario != "embargo" &&
      scenario != "unknown-token" && scenario != "disconnect" &&
      scenario != "pipelined-provide" && scenario != "pipelined-provide-chain") {
    fprintf(stderr, "unknown scenario: %s\n", scenario.cStr());
    return 2;
  }

  try {
    return runDriver(host, port, scenario);
  } catch (kj::Exception& e) {
    printf("Bail out! kj exception: %s\n", e.getDescription().cStr());
    fflush(stdout);
    return 1;
  } catch (std::exception& e) {
    printf("Bail out! %s\n", e.what());
    fflush(stdout);
    return 1;
  }
}

#else  // CAPNP_VERSION < 2000000

int main() {
  fprintf(stderr,
      "l3_vatc_client requires Cap'n Proto C++ >= 2.0.0 for generic VatNetwork Level-3 hooks\n");
  return 1;
}

#endif
