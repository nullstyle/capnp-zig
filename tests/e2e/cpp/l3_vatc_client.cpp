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
//     host and fills ThirdPartyCompletion{token}; park scenarios can rewrite
//     it to an unmatchable high-bit token or the next sequential provision.
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
//   unknown-token completion token rewritten with an unmatchable high bit;
//                 the Accept must PARK (the
//                 rendezvous is order-independent, rpc.h:483-492) rather than
//                 fail, and must not wedge either leg to the host.
//   disconnect    happy, then both host connections are dropped abruptly (no
//                 clean shutdown) via _exit(0).
//   park-fairness one recipient fills its one-entry test quota with an
//                 unmatched Accept and gets a second park refused; the sibling
//                 recipient completes a legitimate reverse-direction handoff
//                 while the first park is still live, then ordinary traffic
//                 expires it and recovers the attacker's share.
//   park-adopt    Accept arrives first and remains parked just long enough for
//                 the matching Provide to adopt it and complete the call.
//   park-expiry   an unmatched Accept crosses a short explicit TTL; its call
//                 pipeline receives the same terminal expiry exception.
//   pipelined-provide, pipelined-provide-chain
//                 B introduces a still-PIPELINED result cap that re-resolves
//                 to B's own local cap — one the host merely IMPORTS. With
//                 the receiverHosted lift the host must SERVE the Accept
//                 (deferred-Release import pinning): the accepted cap works
//                 and reaches B's local capability. See the scenario branch
//                 in runDriver for the two shapes.
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

  // How `connectToIntroduced` rewrites the completion token before sending the
  // Accept. Rewriting it is how this harness controls the RENDEZVOUS: the token
  // is the only identity linking an Accept to a Provide.
  enum class TokenRewrite {
    // Spec-correct: the Accept names the Provide that introduced this cap.
    none,
    // Names a Provide that will never exist, so the Accept parks until expiry
    // or terminal transport cleanup. A high-bit flip, chosen so it cannot
    // collide with any minted token.
    unmatchable,
    // Names the token the NEXT introduction is about to register. `newToken()`
    // hands out small sequential values, so `+1` is exactly "the next
    // Provide". The Accept therefore arrives BEFORE the Provide it names and
    // must park, then be adopted when that Provide lands — the order-
    // independent rendezvous of rpc.h:483-492, driven deliberately.
    next_provision,
  };
  TokenRewrite tokenRewrite = TokenRewrite::none;
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
  switch (vat.network.tokenRewrite) {
    case DriverNetwork::TokenRewrite::none:
      break;
    case DriverNetwork::TokenRewrite::unmatchable:
      // A high-bit flip, NOT `+= 1`. Sequential tokens mean `+1` names the next
      // introduction's Provide, which would adopt-and-serve this Accept instead
      // of parking it forever — silently turning a park cell into a rendezvous
      // cell. That distinction cost `park-expiry` a debugging round; it is now
      // the difference between these two modes.
      token += 0x8000000000000000ull;
      break;
    case DriverNetwork::TokenRewrite::next_provision:
      token += 1;
      break;
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

class LocalNumberImpl final: public Number::Server {
  // B's local capability for the pipelined-provide scenarios: the cap the
  // host re-resolves the provided pipeline to (an IMPORT from the host's
  // perspective). With the receiverHosted lift the host SERVES the Accept by
  // proxying back to this cap over the introducer leg, so it must answer for
  // real. Returns 43 — deliberately distinct from host-Carol's 42 — so a
  // passing call proves the accepted cap reached B's OWN capability, not a
  // host-side one.
public:
  int calls = 0;

  kj::Promise<void> getNumber(GetNumberContext context) override {
    ++calls;
    context.getResults().setN(43);
    return kj::READY_NOW;
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
  // All three park-flavoured cells send an Accept whose token names no
  // provision that exists YET, so the host must park it. They differ only in
  // what happens next: `unknown-token` stays parked for the live observation
  // window and is reclaimed when its transport closes,
  // `park-expiry` lets the L9 TTL evict it, and `park-adopt` names the NEXT
  // introduction's Provide so the park is later ADOPTED and served.
  if (scenario == "unknown-token" || scenario == "park-expiry" ||
      scenario == "park-fairness") {
    shared.tokenRewrite = DriverNetwork::TokenRewrite::unmatchable;
  } else if (scenario == "park-adopt") {
    shared.tokenRewrite = DriverNetwork::TokenRewrite::next_provision;
  }

  DriverVat vatA(shared, "a");
  DriverVat vatB(shared, "b");
  vatA.partner = &vatB;
  vatB.partner = &vatA;
  vatB.adoptHostStream(kj::mv(streamB));
  vatA.adoptHostStream(kj::mv(streamA));

  auto holderOwn = kj::heap<HolderImpl>();
  HolderImpl& holder = *holderOwn;
  auto holderAOwn = kj::heap<HolderImpl>();
  HolderImpl& holderA = *holderAOwn;
  auto rpcB = capnp::makeRpcServer(vatB, capnp::Capability::Client(kj::mv(holderOwn)));
  // A normally only consumes B's bootstrap, but making its endpoint a server
  // too lets park-fairness reverse the roles: B can accept a capability A
  // imported from the host, proving an attacker on A cannot consume B's share.
  auto rpcA = capnp::makeRpcServer(vatA, capnp::Capability::Client(kj::mv(holderAOwn)));

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
    //                            receiverHosted cap (lift site 1: the stored
    //                            `.local{receiverHosted}` target, pinned at
    //                            Provide registration).
    //   pipelined-provide-chain  held = pipeline of Q1 = echo(pipeline of
    //                            Q2); the Q1 answer carries a chained
    //                            receiverAnswer{Q2} descriptor, so the host
    //                            stores .promised ops and only re-resolves
    //                            at ACCEPT time (lift site 2, the
    //                            .promised -> .imported arm, pinned at serve
    //                            time).
    //
    // With the receiverHosted lift both shapes must be SERVED: the Accept
    // resolves cleanly and the accepted cap reaches B's OWN capability over
    // the host's introducer leg (deferred-Release import pinning keeps that
    // import alive across the handoff window). These cells previously
    // asserted the fail-closed refusal by name; this is the rewrite.
    auto localOwn = kj::heap<LocalNumberImpl>();
    LocalNumberImpl& localNum = *localOwn;
    capnp::Capability::Client bCap(kj::mv(localOwn));

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
    // promisedAnswer target on the B leg). The Response is scoped so the
    // ONLY surviving ref to the accepted cap is `accepted` — the release
    // ceremony below must drive a real wire Release.
    capnp::MallocMessageBuilder bIdMsgSpike(8);
    auto bIdSpike = bIdMsgSpike.initRoot<VatId>();
    bIdSpike.setHost("b");
    auto holderCapSpike = rpcA.bootstrap(bIdSpike.asReader());
    Number::Client accepted(nullptr);
    {
      auto resp = holderCapSpike
          .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      accepted = readRootCap(resp).castAs<Number>();
    }
    tap.ok(true, "A received the introduced third-party cap from B");

    // First use forces the lazy Accept, which the lift now SERVES.
    //
    // The probe is deliberately a PIPELINED call, not whenResolved(): rpc.c++
    // sends this Call on the Accept question's promisedAnswer without waiting
    // for the Accept's own Return, so it exercises the broken-pipeline rule as
    // well as the serve. That matters historically — this exact wait HUNG
    // FOREVER when the host dropped calls pipelined on an already-failed
    // answer, and the fix that closed it keeps its cross-impl teeth here in the
    // ANSWERED direction. The FAILED-answer directions (both of them: drained
    // while queued, and answered from the recorded exception after the Return)
    // are covered by the `park-expiry` scenario, which drives a real refusal
    // via the parked-accept TTL now that the lift turned this scenario's own
    // refusal into a success.
    bool threw = false;
    kj::String desc = kj::str("<no exception>");
    KJ_IF_SOME(e, kj::runCatchingExceptions([&]() {
      accepted.getNumberRequest().send().wait(ws);
    })) {
      threw = true;
      desc = kj::str(e.getDescription());
    }
    printf("# accept outcome: %s\n", threw ? desc.cStr() : "resolved");
    fflush(stdout);
    tap.ok(!threw, "accepting the pipelined-provided cap succeeded (Accept served)");

    // The accepted cap WORKS, and it reaches B's own local capability —
    // 43, not host-Carol's 42.
    uint32_t n = accepted.getNumberRequest().send().wait(ws).getN();
    tap.ok(n == 43, "call on the accepted cap returned 43 (B's local cap)");
    // TWO invocations, not one: the probe above is itself a real pipelined
    // call, not a whenResolved(). Both must land on B's cap — the first
    // proves a call pipelined on the Accept question is answered, the second
    // that the settled capability still routes to the same place.
    tap.ok(localNum.calls == 2, "B's local capability was invoked by both the pipelined probe and the settled call");

    // Release ceremony (the `happy` pattern): drop A's only ref to the
    // accepted cap and turn the kj loop so the queued Release actually
    // reaches the host — its side asserts the proxy export died and the
    // transient handoff state drained.
    accepted = nullptr;
    io.provider->getTimer().afterDelay(250 * kj::MILLISECONDS).wait(ws);
    tap.ok(true, "released the accepted cap and flushed the Release to the host");

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

  if (scenario == "park-fairness") {
    auto& timer = io.provider->getTimer();

    // Fill A's deliberately tiny (one-entry) park share. The high-bit token
    // rewrite guarantees neither Accept can match a real provision.
    Number::Client attackerFirst(nullptr);
    {
      auto resp = holderCap
          .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      attackerFirst = readRootCap(resp).castAs<Number>();
    }
    auto firstProbe = attackerFirst.getNumberRequest().send();

    Number::Client attackerSecond(nullptr);
    {
      auto resp = holderCap
          .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      attackerSecond = readRootCap(resp).castAs<Number>();
    }
    auto secondProbe = attackerSecond.getNumberRequest().send();
    auto quotaOutcome = secondProbe
        .then([](auto&&) { return kj::str("<resolved>"); },
              [](kj::Exception&& e) { return kj::str("exception: ", e.getDescription()); })
        .exclusiveJoin(timer.afterDelay(500 * kj::MILLISECONDS)
            .then([]() { return kj::str("<timed out>"); }))
        .wait(ws);
    printf("# per-peer quota outcome: %s\n", quotaOutcome.cStr());
    fflush(stdout);
    tap.ok(strcmp(quotaOutcome.cStr(), "<timed out>") != 0,
        "the second unmatched Accept was answered (did not park forever)");
    tap.ok(strncmp(quotaOutcome.cStr(), "exception:", 10) == 0,
        "the attacker's second park was refused at its per-peer quota");

    // Reverse the ordinary scenario while A's first park is still live: A
    // imports Carol from its host leg and introduces that cap to B. Switching
    // back to a correct completion token makes B the accepting peer, so this
    // is a legitimate sibling handoff under active pressure, not a liveness
    // call on an unrelated connection.
    shared.tokenRewrite = DriverNetwork::TokenRewrite::none;
    auto returnerA = rpcA.bootstrap(hostId.asReader());
    auto returnerAResp = returnerA
        .typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {})
        .send().wait(ws);
    holderA.held = readRootCap(returnerAResp);
    capnp::MallocMessageBuilder aIdMsg(8);
    auto aId = aIdMsg.initRoot<VatId>();
    aId.setHost("a");
    auto holderAFromB = rpcB.bootstrap(aId.asReader());
    Number::Client siblingAccepted(nullptr);
    {
      auto resp = holderAFromB
          .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
          .send().wait(ws);
      siblingAccepted = readRootCap(resp).castAs<Number>();
    }
    uint32_t n = siblingAccepted.getNumberRequest().send().wait(ws).getN();
    tap.ok(n == 42, "the sibling peer completed a legitimate reverse-direction handoff");
    tap.ok(!firstProbe.poll(ws),
        "the sibling handoff completed while the attacker's first park remained live");

    siblingAccepted = nullptr;
    timer.afterDelay(250 * kj::MILLISECONDS).wait(ws);
    tap.ok(true, "released the sibling's accepted cap and flushed its Release");

    // Outlive the short test TTL, then send ordinary traffic on A's host leg.
    // Expiry is checked at the start of every inbound-frame path, so this
    // ordinary call — not another Accept — must reclaim the first park.
    timer.afterDelay(3250 * kj::MILLISECONDS).wait(ws);
    auto sweepDriver = returnerA
        .typelessRequest(RETURNER_INTERFACE_ID, RETURNER_METHOD_ID, kj::none, {})
        .send().wait(ws);
    (void)sweepDriver;
    tap.ok(true, "ordinary recipient traffic remained live and drove the expiry sweep");

    auto expiryOutcome = firstProbe
        .then([](auto&&) { return kj::str("<resolved>"); },
              [](kj::Exception&& e) { return kj::str("exception: ", e.getDescription()); })
        .exclusiveJoin(timer.afterDelay(5 * kj::SECONDS)
            .then([]() { return kj::str("<timed out>"); }))
        .wait(ws);
    printf("# first parked Accept outcome: %s\n", expiryOutcome.cStr());
    fflush(stdout);
    tap.ok(strncmp(expiryOutcome.cStr(), "exception:", 10) == 0,
        "the first parked Accept expired and refunded A's share");

    tap.plan();
    return tap.failed ? 1 : 0;
  }

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

    if (scenario == "park-adopt") {
      // THE ORDER-INDEPENDENT RENDEZVOUS, CROSS-IMPL (Accept BEFORE Provide).
      //
      // rpc.h:483-492: "The two calls can happen in any order;
      // `completeThirdParty()` will wait for a corresponding
      // `awaitThirdParty()` if it hasn't happened already." `unknown-token`
      // proves the waiting half — an Accept naming no provision parks. This
      // proves the OTHER half: a parked Accept must be ADOPTED and served when
      // the Provide it names finally arrives.
      //
      // The mechanism is the token rewrite: A's Accept for the first
      // introduction names token+1, which is exactly what B's NEXT introduction
      // registers. So the Accept lands first and parks, and the second Provide
      // adopts it. No timing hook and no delay is involved — this rests on the
      // driver's own token arithmetic, so it is deterministic.
      //
      // (This is the shape the `+= 1` token-corruption bug was exercising by
      // accident before it was fixed; it silently made `park-expiry` a
      // rendezvous cell. Here it is the deliberate subject.)
      auto probe = carol.getNumberRequest().send();

      // Nothing can resolve that probe yet: its Accept is parked on a provision
      // that does not exist. Prove the park is real before creating it, rather
      // than assuming it.
      //
      // `poll` and NOT `exclusiveJoin(timer)`: exclusiveJoin CANCELS the loser,
      // so racing the probe against a timeout would cancel the very call under
      // test, and `.then()` consumes the promise so a second use is a
      // use-after-move. Both of those were live here and the driver segfaulted
      // (exit 139). `poll` turns the event loop and reports readiness while
      // leaving the promise intact and owned.
      auto& timer = io.provider->getTimer();
      timer.afterDelay(2 * kj::SECONDS).wait(ws);
      tap.ok(!probe.poll(ws),
          "the Accept parked: no Return while the provision it names does not exist");

      // Now make it exist. Fetching from B's holder again re-introduces the
      // cap, and THAT Provide registers the token the parked Accept named.
      Number::Client second(nullptr);
      {
        auto resp2 = holderCap
            .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
            .send().wait(ws);
        second = readRootCap(resp2).castAs<Number>();
      }
      tap.ok(true, "B introduced a second cap, registering the awaited token");

      // Bounded: a hang here is the regression (adoption never happened), so it
      // must fail rather than wedge the lane until the runner's timeout. NOW
      // consuming the probe is correct — this is its single terminal use.
      auto outcome = probe
          .then([](capnp::Response<Number::GetNumberResults> r) { return kj::str(r.getN()); },
                [](kj::Exception&& e) { return kj::str("exception: ", e.getDescription()); })
          .exclusiveJoin(timer.afterDelay(10 * kj::SECONDS)
              .then([]() { return kj::str("<timed out>"); }))
          .wait(ws);
      printf("# park-adopt outcome: %s\n", outcome.cStr());
      fflush(stdout);

      tap.ok(strcmp(outcome.cStr(), "<timed out>") != 0,
          "the parked Accept was adopted by the later Provide (did not hang)");
      tap.ok(strcmp(outcome.cStr(), "42") == 0,
          "the adopted Accept was SERVED and the call returned 42");

      // Release ceremony (the `happy` pattern): an adopted-then-served
      // capability must have the same lifecycle as a normally-served one, so
      // drop A's refs and turn the loop until the Release reaches the host. Its
      // side asserts the cross-peer proxy export actually died — without this,
      // the proxy link survives to the checkpoint and only teardown reaps it,
      // which would prove nothing about Release handling on an adopted handoff.
      carol = nullptr;
      second = nullptr;
      timer.afterDelay(250 * kj::MILLISECONDS).wait(ws);
      tap.ok(true, "released the adopted cap and flushed the Release to the host");
    } else if (scenario == "park-expiry") {
      // THE FAILED-ANSWER ARM OF THE BROKEN-PIPELINE RULE, CROSS-IMPL.
      //
      // `carol` is an unresolved Accept promise whose completion token matches
      // no Provide, so the host parks the Accept. Calling on it now makes
      // rpc.c++ send a Call PIPELINED on that Accept question (promisedAnswer)
      // without waiting for its Return — exactly the shape whose ANSWERED
      // direction `pipelined-provide` covers.
      //
      // The host then evicts the parked Accept on its L9 TTL and answers that
      // question with an exception. The pipelined call must be answered with a
      // COPY of that same exception. Before the fix in v0.7.0 it was dropped,
      // and this wait hung until the lane's timeout killed it.
      auto pipelined = carol.getNumberRequest().send();

      // Outlive the host's TTL. No background task runs the sweep, so nothing
      // has happened yet; the next inbound frame will perform the due check.
      auto& timer = io.provider->getTimer();
      timer.afterDelay(1 * kj::SECONDS).wait(ws);

      // The SECOND Accept, which is what actually drives the sweep. Fetching
      // from B's holder again re-introduces the cap, so A sends a fresh Accept
      // (with a freshly corrupted token, so this one parks in its turn).
      Number::Client second(nullptr);
      {
        auto resp2 = holderCap
            .typelessRequest(HOLDER_INTERFACE_ID, HOLDER_METHOD_ID, kj::none, {})
            .send().wait(ws);
        second = readRootCap(resp2).castAs<Number>();
      }
      // USE it: rpc.c++ sends the Accept lazily, on first call, so merely
      // holding the second cap sends nothing and the sweep never runs. This
      // probe is fire-and-forget — its own token is corrupted too, so it parks
      // in its turn and is never answered. Kept alive only so it is not
      // cancelled before the Accept reaches the wire.
      auto secondProbe = second.getNumberRequest().send();

      // A SECOND pipelined call on the FIRST (about to be evicted) question,
      // queued in the same turn as the Accept above so the event loop is not
      // turned in between. Frame order on one connection is FIFO, so the host
      // reads: Accept#2 -> [sweep evicts Accept#1, Return(exception) for it]
      // -> this Call. It therefore arrives on an answer that has ALREADY
      // failed, which is the other half of the broken-pipeline rule and the
      // half a conformant client never sends by accident: once A has PROCESSED
      // the exception Return its promise is broken and it fails such calls
      // locally, without a frame. Not turning the loop here is what makes this
      // deterministic rather than a race.
      auto late = carol.getNumberRequest().send();
      tap.ok(true, "A used a second introduced cap (its Accept drives the TTL sweep)");

      // Bounded: a hang here is the regression, so it must fail rather than
      // wedge the lane until the runner's timeout.
      bool settled = false;
      bool threw = false;
      kj::String desc = kj::str("<no exception>");
      auto outcome = pipelined
          .then([](auto&&) { return kj::str("<resolved>"); },
                [](kj::Exception&& e) { return kj::str(e.getDescription()); })
          .exclusiveJoin(timer.afterDelay(5 * kj::SECONDS)
              .then([]() { return kj::str("<timed out>"); }))
          .wait(ws);
      settled = (strcmp(outcome.cStr(), "<timed out>") != 0);
      threw = settled && (strcmp(outcome.cStr(), "<resolved>") != 0);
      desc = kj::mv(outcome);
      printf("# pipelined-on-failed-answer outcome: %s\n", desc.cStr());
      fflush(stdout);

      tap.ok(settled,
          "the call pipelined on the evicted Accept was ANSWERED (did not hang)");
      tap.ok(threw,
          "it was answered with an exception, not a bogus result");
      // The exception is a COPY of the answer's, not a fresh generic one —
      // this is what separates the rule from "the connection broke".
      tap.ok(strstr(desc.cStr(), "parked accept expired") != nullptr,
          "the exception is the failed answer's own (\"parked accept expired\")");

      // The after-the-Return arm. Same bound, same expectation: it must be
      // ANSWERED with a copy of that answer's exception, not dropped.
      auto lateOutcome = late
          .then([](auto&&) { return kj::str("<resolved>"); },
                [](kj::Exception&& e) { return kj::str(e.getDescription()); })
          .exclusiveJoin(timer.afterDelay(5 * kj::SECONDS)
              .then([]() { return kj::str("<timed out>"); }))
          .wait(ws);
      printf("# call-on-already-failed-answer outcome: %s\n", lateOutcome.cStr());
      fflush(stdout);
      tap.ok(strcmp(lateOutcome.cStr(), "<timed out>") != 0,
          "a call arriving on an ALREADY-failed answer was answered (did not hang)");
      tap.ok(strstr(lateOutcome.cStr(), "parked accept expired") != nullptr,
          "it too got a copy of the failed answer's own exception");

      // Liveness after the eviction: the introducer leg still serves calls.
      uint32_t viaB = holder.held.castAs<Number>().getNumberRequest().send().wait(ws).getN();
      tap.ok(viaB == 42, "introducer leg still serves calls after the eviction");
      (void)second;
      (void)secondProbe;
      (void)late;
    } else if (scenario == "unknown-token") {
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
                    "            park-expiry | park-adopt | park-fairness |\n"
                    "            pipelined-provide |\n"
                    "            pipelined-provide-chain\n");
    return 2;
  }
  kj::StringPtr host = argv[1];
  kj::StringPtr port = argv[2];
  kj::StringPtr scenario = argv[3];

  if (scenario != "happy" && scenario != "embargo" &&
      scenario != "unknown-token" && scenario != "disconnect" &&
      scenario != "park-expiry" && scenario != "park-adopt" &&
      scenario != "park-fairness" &&
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
