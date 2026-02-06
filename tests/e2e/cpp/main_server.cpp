// C++ reference RPC server implementing all game-dev test interfaces.
// Uses KJ async + Cap'n Proto RPC.

#include <capnp/rpc-twoparty.h>
#include <capnp/message.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/main.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "game_types.capnp.h"
#include "game_world.capnp.h"
#include "chat.capnp.h"
#include "inventory.capnp.h"
#include "matchmaking.capnp.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static float distance(float x1, float y1, float z1, float x2, float y2, float z2) {
  float dx = x1 - x2, dy = y1 - y2, dz = z1 - z2;
  return std::sqrt(dx * dx + dy * dy + dz * dz);
}

// ---------------------------------------------------------------------------
// GameWorld implementation
// ---------------------------------------------------------------------------

struct EntityData {
  uint64_t id;
  EntityKind kind;
  std::string name;
  float x, y, z;
  int32_t health;
  int32_t maxHealth;
  Faction faction;
  bool alive;
};

static void fillEntity(EntityData const& e, Entity::Builder b) {
  b.getId().setId(e.id);
  b.setKind(e.kind);
  b.setName(e.name.c_str());
  auto pos = b.initPosition();
  pos.setX(e.x);
  pos.setY(e.y);
  pos.setZ(e.z);
  b.setHealth(e.health);
  b.setMaxHealth(e.maxHealth);
  b.setFaction(e.faction);
  b.setAlive(e.alive);
}

class GameWorldImpl final : public GameWorld::Server {
public:
  kj::Promise<void> spawnEntity(SpawnEntityContext context) override {
    auto req = context.getParams().getRequest();
    EntityData e;
    e.id = nextEntityId_++;
    e.kind = req.getKind();
    e.name = req.getName().cStr();
    auto pos = req.getPosition();
    e.x = pos.getX();
    e.y = pos.getY();
    e.z = pos.getZ();
    e.maxHealth = req.getMaxHealth();
    e.health = e.maxHealth;
    e.faction = req.getFaction();
    e.alive = true;
    entities_[e.id] = e;

    auto results = context.getResults();
    fillEntity(e, results.initEntity());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> despawnEntity(DespawnEntityContext context) override {
    uint64_t id = context.getParams().getId().getId();
    auto it = entities_.find(id);
    if (it == entities_.end()) {
      context.getResults().setStatus(StatusCode::NOT_FOUND);
    } else {
      entities_.erase(it);
      context.getResults().setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> getEntity(GetEntityContext context) override {
    uint64_t id = context.getParams().getId().getId();
    auto it = entities_.find(id);
    auto results = context.getResults();
    if (it == entities_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
    } else {
      fillEntity(it->second, results.initEntity());
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> moveEntity(MoveEntityContext context) override {
    auto params = context.getParams();
    uint64_t id = params.getId().getId();
    auto it = entities_.find(id);
    auto results = context.getResults();
    if (it == entities_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
    } else {
      auto pos = params.getNewPosition();
      it->second.x = pos.getX();
      it->second.y = pos.getY();
      it->second.z = pos.getZ();
      fillEntity(it->second, results.initEntity());
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> damageEntity(DamageEntityContext context) override {
    auto params = context.getParams();
    uint64_t id = params.getId().getId();
    int32_t amount = params.getAmount();
    auto it = entities_.find(id);
    auto results = context.getResults();
    if (it == entities_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
      results.setKilled(false);
    } else {
      auto& e = it->second;
      e.health -= amount;
      bool killed = false;
      if (e.health <= 0) {
        e.health = 0;
        e.alive = false;
        killed = true;
      }
      fillEntity(e, results.initEntity());
      results.setKilled(killed);
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> queryArea(QueryAreaContext context) override {
    auto params = context.getParams();
    auto query = params.getQuery();
    auto center = query.getCenter();
    float radius = query.getRadius();

    std::vector<EntityData const*> matches;
    for (auto& [_, e] : entities_) {
      float d = distance(e.x, e.y, e.z, center.getX(), center.getY(), center.getZ());
      if (d > radius) continue;
      auto filter = query.getFilter();
      switch (filter.which()) {
        case AreaQuery::Filter::ALL:
          matches.push_back(&e);
          break;
        case AreaQuery::Filter::BY_KIND:
          if (e.kind == filter.getByKind()) matches.push_back(&e);
          break;
        case AreaQuery::Filter::BY_FACTION:
          if (e.faction == filter.getByFaction()) matches.push_back(&e);
          break;
      }
    }

    auto results = context.getResults();
    auto list = results.initEntities(matches.size());
    for (size_t i = 0; i < matches.size(); ++i) {
      fillEntity(*matches[i], list[i]);
    }
    results.setCount(static_cast<uint32_t>(matches.size()));
    return kj::READY_NOW;
  }

private:
  uint64_t nextEntityId_ = 1;
  std::map<uint64_t, EntityData> entities_;
};

// ---------------------------------------------------------------------------
// Chat implementation
// ---------------------------------------------------------------------------

struct ChatMsgData {
  std::string senderName;
  uint64_t senderId;
  Faction senderFaction;
  uint16_t senderLevel;
  std::string content;
  int64_t timestamp;
  enum Kind { NORMAL, EMOTE, SYSTEM, WHISPER } kind;
  uint64_t whisperTarget;
};

static void fillChatMessage(ChatMsgData const& m, ChatMessage::Builder b) {
  auto sender = b.initSender();
  sender.getId().setId(m.senderId);
  sender.setName(m.senderName.c_str());
  sender.setFaction(m.senderFaction);
  sender.setLevel(m.senderLevel);
  b.setContent(m.content.c_str());
  b.getTimestamp().setUnixMillis(m.timestamp);
  switch (m.kind) {
    case ChatMsgData::NORMAL: b.getKind().setNormal(); break;
    case ChatMsgData::EMOTE:  b.getKind().setEmote(); break;
    case ChatMsgData::SYSTEM: b.getKind().setSystem(); break;
    case ChatMsgData::WHISPER: {
      auto target = b.getKind().initWhisper();
      target.setId(m.whisperTarget);
      break;
    }
  }
}

struct RoomData {
  uint64_t id;
  std::string name;
  std::string topic;
  uint32_t memberCount;
  std::vector<ChatMsgData> history;
  // Track the "current user" per room for simplicity
  std::string currentUserName;
  uint64_t currentUserId;
  Faction currentUserFaction;
  uint16_t currentUserLevel;
};

class ChatRoomImpl final : public ChatRoom::Server {
public:
  explicit ChatRoomImpl(std::shared_ptr<RoomData> room) : room_(std::move(room)) {}

  kj::Promise<void> sendMessage(SendMessageContext context) override {
    auto content = context.getParams().getContent().cStr();
    int64_t now = 1700000000000LL + static_cast<int64_t>(room_->history.size()) * 1000;
    ChatMsgData msg;
    msg.senderName = room_->currentUserName;
    msg.senderId = room_->currentUserId;
    msg.senderFaction = room_->currentUserFaction;
    msg.senderLevel = room_->currentUserLevel;
    msg.content = content;
    msg.timestamp = now;
    msg.kind = ChatMsgData::NORMAL;
    msg.whisperTarget = 0;
    room_->history.push_back(msg);

    auto results = context.getResults();
    fillChatMessage(msg, results.initMessage());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> sendEmote(SendEmoteContext context) override {
    auto content = context.getParams().getContent().cStr();
    int64_t now = 1700000000000LL + static_cast<int64_t>(room_->history.size()) * 1000;
    ChatMsgData msg;
    msg.senderName = room_->currentUserName;
    msg.senderId = room_->currentUserId;
    msg.senderFaction = room_->currentUserFaction;
    msg.senderLevel = room_->currentUserLevel;
    msg.content = content;
    msg.timestamp = now;
    msg.kind = ChatMsgData::EMOTE;
    msg.whisperTarget = 0;
    room_->history.push_back(msg);

    auto results = context.getResults();
    fillChatMessage(msg, results.initMessage());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> getHistory(GetHistoryContext context) override {
    uint32_t limit = context.getParams().getLimit();
    auto& hist = room_->history;
    uint32_t start = hist.size() > limit ? static_cast<uint32_t>(hist.size()) - limit : 0;
    uint32_t count = static_cast<uint32_t>(hist.size()) - start;

    auto results = context.getResults();
    auto list = results.initMessages(count);
    for (uint32_t i = 0; i < count; ++i) {
      fillChatMessage(hist[start + i], list[i]);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> getInfo(GetInfoContext context) override {
    auto info = context.getResults().initInfo();
    info.getId().setId(room_->id);
    info.setName(room_->name.c_str());
    info.setMemberCount(room_->memberCount);
    info.setTopic(room_->topic.c_str());
    return kj::READY_NOW;
  }

  kj::Promise<void> leave(LeaveContext context) override {
    if (room_->memberCount > 0) room_->memberCount--;
    context.getResults().setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

private:
  std::shared_ptr<RoomData> room_;
};

class ChatServiceImpl final : public ChatService::Server {
public:
  kj::Promise<void> createRoom(CreateRoomContext context) override {
    auto params = context.getParams();
    auto room = std::make_shared<RoomData>();
    room->id = nextRoomId_++;
    room->name = params.getName().cStr();
    room->topic = params.getTopic().cStr();
    room->memberCount = 0;
    rooms_[room->name] = room;

    auto results = context.getResults();
    results.setRoom(kj::heap<ChatRoomImpl>(room));
    auto info = results.initInfo();
    info.getId().setId(room->id);
    info.setName(room->name.c_str());
    info.setMemberCount(room->memberCount);
    info.setTopic(room->topic.c_str());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> joinRoom(JoinRoomContext context) override {
    auto params = context.getParams();
    std::string name = params.getName().cStr();
    auto it = rooms_.find(name);
    auto results = context.getResults();
    if (it == rooms_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
    } else {
      auto& room = it->second;
      room->memberCount++;
      auto player = params.getPlayer();
      room->currentUserName = player.getName().cStr();
      room->currentUserId = player.getId().getId();
      room->currentUserFaction = player.getFaction();
      room->currentUserLevel = player.getLevel();
      results.setRoom(kj::heap<ChatRoomImpl>(room));
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> listRooms(ListRoomsContext context) override {
    auto results = context.getResults();
    auto list = results.initRooms(rooms_.size());
    size_t i = 0;
    for (auto& [name, room] : rooms_) {
      list[i].getId().setId(room->id);
      list[i].setName(room->name.c_str());
      list[i].setMemberCount(room->memberCount);
      list[i].setTopic(room->topic.c_str());
      ++i;
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> whisper(WhisperContext context) override {
    auto params = context.getParams();
    auto from = params.getFrom();
    auto to = params.getTo();

    ChatMsgData msg;
    msg.senderName = from.getName().cStr();
    msg.senderId = from.getId().getId();
    msg.senderFaction = from.getFaction();
    msg.senderLevel = from.getLevel();
    msg.content = params.getContent().cStr();
    msg.timestamp = 1700000000000LL;
    msg.kind = ChatMsgData::WHISPER;
    msg.whisperTarget = to.getId();

    auto results = context.getResults();
    fillChatMessage(msg, results.initMessage());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

private:
  uint64_t nextRoomId_ = 1;
  std::map<std::string, std::shared_ptr<RoomData>> rooms_;
};

// ---------------------------------------------------------------------------
// Inventory implementation
// ---------------------------------------------------------------------------

struct ItemData {
  uint64_t itemId;
  std::string name;
  Rarity rarity;
  uint16_t level;
  uint32_t stackSize;
  std::vector<std::pair<std::string, int32_t>> attributes;
};

struct SlotData {
  uint16_t slotIndex;
  ItemData item;
  uint32_t quantity;
};

static void fillItem(ItemData const& item, Item::Builder b) {
  b.getId().setId(item.itemId);
  b.setName(item.name.c_str());
  b.setRarity(item.rarity);
  b.setLevel(item.level);
  b.setStackSize(item.stackSize);
  auto attrs = b.initAttributes(item.attributes.size());
  for (size_t i = 0; i < item.attributes.size(); ++i) {
    attrs[i].setName(item.attributes[i].first.c_str());
    attrs[i].setValue(item.attributes[i].second);
  }
}

static void fillSlot(SlotData const& s, InventorySlot::Builder b) {
  b.setSlotIndex(s.slotIndex);
  fillItem(s.item, b.initItem());
  b.setQuantity(s.quantity);
}

struct PlayerInventory {
  uint64_t ownerId;
  std::vector<SlotData> slots;
  uint16_t capacity;
};

struct TradeData {
  uint64_t initiatorId;
  uint64_t targetId;
  std::vector<uint16_t> initiatorSlots;
  std::vector<uint16_t> targetSlots;
  bool initiatorAccepted;
  bool targetAccepted;
  TradeState state;
};

class TradeSessionImpl final : public TradeSession::Server {
public:
  TradeSessionImpl(std::shared_ptr<TradeData> trade, bool isInitiator,
                   std::map<uint64_t, PlayerInventory>* inventories)
    : trade_(std::move(trade)), isInitiator_(isInitiator), inventories_(inventories) {}

  kj::Promise<void> offerItems(OfferItemsContext context) override {
    auto params = context.getParams();
    auto slotsList = params.getSlots();
    auto& mySlots = isInitiator_ ? trade_->initiatorSlots : trade_->targetSlots;
    mySlots.clear();
    for (auto s : slotsList) {
      mySlots.push_back(s);
    }

    auto results = context.getResults();
    auto offer = results.initOffer();
    // Build the offered items list from the inventory
    uint64_t myId = isInitiator_ ? trade_->initiatorId : trade_->targetId;
    auto invIt = inventories_->find(myId);
    if (invIt != inventories_->end()) {
      auto offeredList = offer.initOfferedItems(mySlots.size());
      for (size_t i = 0; i < mySlots.size(); ++i) {
        for (auto& slot : invIt->second.slots) {
          if (slot.slotIndex == mySlots[i]) {
            fillSlot(slot, offeredList[i]);
            break;
          }
        }
      }
    }
    offer.setAccepted(isInitiator_ ? trade_->initiatorAccepted : trade_->targetAccepted);
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> removeItems(RemoveItemsContext context) override {
    auto params = context.getParams();
    auto slotsToRemove = params.getSlots();
    auto& mySlots = isInitiator_ ? trade_->initiatorSlots : trade_->targetSlots;
    for (auto s : slotsToRemove) {
      mySlots.erase(std::remove(mySlots.begin(), mySlots.end(), s), mySlots.end());
    }

    auto results = context.getResults();
    auto offer = results.initOffer();
    uint64_t myId = isInitiator_ ? trade_->initiatorId : trade_->targetId;
    auto invIt = inventories_->find(myId);
    if (invIt != inventories_->end()) {
      auto offeredList = offer.initOfferedItems(mySlots.size());
      for (size_t i = 0; i < mySlots.size(); ++i) {
        for (auto& slot : invIt->second.slots) {
          if (slot.slotIndex == mySlots[i]) {
            fillSlot(slot, offeredList[i]);
            break;
          }
        }
      }
    }
    offer.setAccepted(false);
    if (isInitiator_) trade_->initiatorAccepted = false;
    else trade_->targetAccepted = false;
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> accept(AcceptContext context) override {
    if (isInitiator_) trade_->initiatorAccepted = true;
    else trade_->targetAccepted = true;

    auto results = context.getResults();
    if (trade_->initiatorAccepted && trade_->targetAccepted) {
      trade_->state = TradeState::ACCEPTED;
    }
    results.setState(trade_->state);
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> confirm(ConfirmContext context) override {
    auto results = context.getResults();
    if (trade_->state == TradeState::ACCEPTED) {
      trade_->state = TradeState::CONFIRMED;
      results.setState(TradeState::CONFIRMED);
      results.setStatus(StatusCode::OK);
    } else {
      results.setState(trade_->state);
      results.setStatus(StatusCode::INVALID_ARGUMENT);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> cancel(CancelContext context) override {
    trade_->state = TradeState::CANCELLED;
    context.getResults().setState(TradeState::CANCELLED);
    return kj::READY_NOW;
  }

  kj::Promise<void> viewOtherOffer(ViewOtherOfferContext context) override {
    auto& otherSlots = isInitiator_ ? trade_->targetSlots : trade_->initiatorSlots;
    uint64_t otherId = isInitiator_ ? trade_->targetId : trade_->initiatorId;
    auto offer = context.getResults().initOffer();
    auto invIt = inventories_->find(otherId);
    if (invIt != inventories_->end()) {
      auto offeredList = offer.initOfferedItems(otherSlots.size());
      for (size_t i = 0; i < otherSlots.size(); ++i) {
        for (auto& slot : invIt->second.slots) {
          if (slot.slotIndex == otherSlots[i]) {
            fillSlot(slot, offeredList[i]);
            break;
          }
        }
      }
    }
    offer.setAccepted(isInitiator_ ? trade_->targetAccepted : trade_->initiatorAccepted);
    return kj::READY_NOW;
  }

  kj::Promise<void> getState(GetStateContext context) override {
    context.getResults().setState(trade_->state);
    return kj::READY_NOW;
  }

private:
  std::shared_ptr<TradeData> trade_;
  bool isInitiator_;
  std::map<uint64_t, PlayerInventory>* inventories_;
};

class InventoryServiceImpl final : public InventoryService::Server {
public:
  kj::Promise<void> getInventory(GetInventoryContext context) override {
    uint64_t playerId = context.getParams().getPlayer().getId();
    auto it = inventories_.find(playerId);
    auto results = context.getResults();
    if (it == inventories_.end()) {
      // Return empty inventory
      auto inv = results.initInventory();
      inv.getOwner().setId(playerId);
      inv.initSlots(0);
      inv.setCapacity(20);
      inv.setUsedSlots(0);
      results.setStatus(StatusCode::OK);
    } else {
      auto& pinv = it->second;
      auto inv = results.initInventory();
      inv.getOwner().setId(pinv.ownerId);
      auto slots = inv.initSlots(pinv.slots.size());
      for (size_t i = 0; i < pinv.slots.size(); ++i) {
        fillSlot(pinv.slots[i], slots[i]);
      }
      inv.setCapacity(pinv.capacity);
      inv.setUsedSlots(static_cast<uint16_t>(pinv.slots.size()));
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> addItem(AddItemContext context) override {
    auto params = context.getParams();
    uint64_t playerId = params.getPlayer().getId();
    auto item = params.getItem();
    uint32_t quantity = params.getQuantity();

    auto& pinv = inventories_[playerId];
    pinv.ownerId = playerId;
    if (pinv.capacity == 0) pinv.capacity = 20;

    SlotData slot;
    slot.slotIndex = static_cast<uint16_t>(pinv.slots.size());
    slot.item.itemId = item.getId().getId();
    slot.item.name = item.getName().cStr();
    slot.item.rarity = item.getRarity();
    slot.item.level = item.getLevel();
    slot.item.stackSize = item.getStackSize();
    for (auto attr : item.getAttributes()) {
      slot.item.attributes.emplace_back(std::string(attr.getName().cStr()), attr.getValue());
    }
    slot.quantity = quantity;
    pinv.slots.push_back(slot);

    auto results = context.getResults();
    fillSlot(slot, results.initSlot());
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> removeItem(RemoveItemContext context) override {
    auto params = context.getParams();
    uint64_t playerId = params.getPlayer().getId();
    uint16_t slotIndex = params.getSlotIndex();
    uint32_t quantity = params.getQuantity();

    auto it = inventories_.find(playerId);
    auto results = context.getResults();
    if (it == inventories_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
      return kj::READY_NOW;
    }
    auto& slots = it->second.slots;
    for (auto sit = slots.begin(); sit != slots.end(); ++sit) {
      if (sit->slotIndex == slotIndex) {
        if (quantity >= sit->quantity) {
          slots.erase(sit);
        } else {
          sit->quantity -= quantity;
        }
        results.setStatus(StatusCode::OK);
        return kj::READY_NOW;
      }
    }
    results.setStatus(StatusCode::NOT_FOUND);
    return kj::READY_NOW;
  }

  kj::Promise<void> startTrade(StartTradeContext context) override {
    auto params = context.getParams();
    uint64_t initiatorId = params.getInitiator().getId();
    uint64_t targetId = params.getTarget().getId();

    auto trade = std::make_shared<TradeData>();
    trade->initiatorId = initiatorId;
    trade->targetId = targetId;
    trade->initiatorAccepted = false;
    trade->targetAccepted = false;
    trade->state = TradeState::PROPOSING;

    auto results = context.getResults();
    results.setSession(kj::heap<TradeSessionImpl>(trade, true, &inventories_));
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> filterByRarity(FilterByRarityContext context) override {
    auto params = context.getParams();
    uint64_t playerId = params.getPlayer().getId();
    Rarity minRarity = params.getMinRarity();

    std::vector<SlotData const*> matches;
    auto it = inventories_.find(playerId);
    if (it != inventories_.end()) {
      for (auto& slot : it->second.slots) {
        if (static_cast<uint16_t>(slot.item.rarity) >= static_cast<uint16_t>(minRarity)) {
          matches.push_back(&slot);
        }
      }
    }

    auto results = context.getResults();
    auto list = results.initItems(matches.size());
    for (size_t i = 0; i < matches.size(); ++i) {
      fillSlot(*matches[i], list[i]);
    }
    return kj::READY_NOW;
  }

private:
  std::map<uint64_t, PlayerInventory> inventories_;
};

// ---------------------------------------------------------------------------
// Matchmaking implementation
// ---------------------------------------------------------------------------

struct MatchData {
  uint64_t matchId;
  GameMode mode;
  MatchState state;
  std::vector<PlayerInfo::Reader> teamA;  // We'll store copies
  std::vector<PlayerInfo::Reader> teamB;
  // Simpler: store serialized player info
  struct PlayerInfoData {
    uint64_t id;
    std::string name;
    Faction faction;
    uint16_t level;
  };
  std::vector<PlayerInfoData> teamAData;
  std::vector<PlayerInfoData> teamBData;
  int64_t createdAt;
  bool ready;
};

struct QueueTicketData {
  uint64_t ticketId;
  std::string playerName;
  uint64_t playerId;
  Faction playerFaction;
  uint16_t playerLevel;
  GameMode mode;
  int64_t enqueuedAt;
};

static void fillPlayerInfo(MatchData::PlayerInfoData const& p, PlayerInfo::Builder b) {
  b.getId().setId(p.id);
  b.setName(p.name.c_str());
  b.setFaction(p.faction);
  b.setLevel(p.level);
}

static void fillMatchInfo(MatchData const& m, MatchInfo::Builder b) {
  b.getId().setId(m.matchId);
  b.setMode(m.mode);
  b.setState(m.state);
  auto tA = b.initTeamA(m.teamAData.size());
  for (size_t i = 0; i < m.teamAData.size(); ++i) {
    fillPlayerInfo(m.teamAData[i], tA[i]);
  }
  auto tB = b.initTeamB(m.teamBData.size());
  for (size_t i = 0; i < m.teamBData.size(); ++i) {
    fillPlayerInfo(m.teamBData[i], tB[i]);
  }
  b.getCreatedAt().setUnixMillis(m.createdAt);
}

class MatchControllerImpl final : public MatchController::Server {
public:
  explicit MatchControllerImpl(std::shared_ptr<MatchData> match)
    : match_(std::move(match)) {}

  kj::Promise<void> getInfo(GetInfoContext context) override {
    fillMatchInfo(*match_, context.getResults().initInfo());
    return kj::READY_NOW;
  }

  kj::Promise<void> signalReady(SignalReadyContext context) override {
    match_->ready = true;
    if (match_->state == MatchState::WAITING) {
      match_->state = MatchState::READY;
    }
    auto results = context.getResults();
    results.setAllReady(true);
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> reportResult(ReportResultContext context) override {
    auto results = context.getResults();
    if (match_->state == MatchState::IN_PROGRESS || match_->state == MatchState::READY) {
      match_->state = MatchState::COMPLETED;
      results.setStatus(StatusCode::OK);
    } else {
      results.setStatus(StatusCode::INVALID_ARGUMENT);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> cancelMatch(CancelMatchContext context) override {
    auto results = context.getResults();
    if (match_->state == MatchState::IN_PROGRESS || match_->state == MatchState::COMPLETED) {
      results.setStatus(StatusCode::INVALID_ARGUMENT);
    } else {
      match_->state = MatchState::CANCELLED;
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

private:
  std::shared_ptr<MatchData> match_;
};

class MatchmakingServiceImpl final : public MatchmakingService::Server {
public:
  kj::Promise<void> enqueue(EnqueueContext context) override {
    auto params = context.getParams();
    auto player = params.getPlayer();
    QueueTicketData ticket;
    ticket.ticketId = nextTicketId_++;
    ticket.playerName = player.getName().cStr();
    ticket.playerId = player.getId().getId();
    ticket.playerFaction = player.getFaction();
    ticket.playerLevel = player.getLevel();
    ticket.mode = params.getMode();
    ticket.enqueuedAt = 1700000000000LL;
    tickets_[ticket.ticketId] = ticket;

    auto results = context.getResults();
    auto t = results.initTicket();
    t.setTicketId(ticket.ticketId);
    auto pi = t.initPlayer();
    pi.getId().setId(ticket.playerId);
    pi.setName(ticket.playerName.c_str());
    pi.setFaction(ticket.playerFaction);
    pi.setLevel(ticket.playerLevel);
    t.setMode(ticket.mode);
    t.getEnqueuedAt().setUnixMillis(ticket.enqueuedAt);
    t.setEstimatedWaitSecs(30);
    results.setStatus(StatusCode::OK);
    return kj::READY_NOW;
  }

  kj::Promise<void> dequeue(DequeueContext context) override {
    uint64_t ticketId = context.getParams().getTicketId();
    auto it = tickets_.find(ticketId);
    if (it == tickets_.end()) {
      context.getResults().setStatus(StatusCode::NOT_FOUND);
    } else {
      tickets_.erase(it);
      context.getResults().setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> findMatch(FindMatchContext context) override {
    auto params = context.getParams();
    auto player = params.getPlayer();
    auto mode = params.getMode();

    auto match = std::make_shared<MatchData>();
    match->matchId = nextMatchId_++;
    match->mode = mode;
    match->state = MatchState::WAITING;
    match->createdAt = 1700000000000LL;
    match->ready = false;

    MatchData::PlayerInfoData pi;
    pi.id = player.getId().getId();
    pi.name = player.getName().cStr();
    pi.faction = player.getFaction();
    pi.level = player.getLevel();
    match->teamAData.push_back(pi);

    // Add a synthetic opponent to team B
    MatchData::PlayerInfoData opponent;
    opponent.id = 9999;
    opponent.name = "BotOpponent";
    opponent.faction = Faction::PIRATES;
    opponent.level = 50;
    match->teamBData.push_back(opponent);

    matches_[match->matchId] = match;

    auto results = context.getResults();
    results.setController(kj::heap<MatchControllerImpl>(match));
    results.getMatchId().setId(match->matchId);
    return kj::READY_NOW;
  }

  kj::Promise<void> getQueueStats(GetQueueStatsContext context) override {
    // Count tickets for the requested mode
    auto mode = context.getParams().getMode();
    uint32_t count = 0;
    for (auto& [_, t] : tickets_) {
      if (t.mode == mode) ++count;
    }
    auto results = context.getResults();
    results.setPlayersInQueue(count);
    results.setAvgWaitSecs(count > 0 ? 15 : 0);
    return kj::READY_NOW;
  }

  kj::Promise<void> getMatchResult(GetMatchResultContext context) override {
    uint64_t matchId = context.getParams().getId().getId();
    auto it = matches_.find(matchId);
    auto results = context.getResults();
    if (it == matches_.end()) {
      results.setStatus(StatusCode::NOT_FOUND);
    } else {
      auto& m = it->second;
      auto result = results.initResult();
      result.getMatchId().setId(m->matchId);
      result.setWinningTeam(0);
      result.setDuration(300);
      auto stats = result.initPlayerStats(m->teamAData.size() + m->teamBData.size());
      size_t idx = 0;
      for (auto& p : m->teamAData) {
        fillPlayerInfo(p, stats[idx].initPlayer());
        stats[idx].setKills(5);
        stats[idx].setDeaths(2);
        stats[idx].setAssists(3);
        stats[idx].setScore(100);
        ++idx;
      }
      for (auto& p : m->teamBData) {
        fillPlayerInfo(p, stats[idx].initPlayer());
        stats[idx].setKills(2);
        stats[idx].setDeaths(5);
        stats[idx].setAssists(1);
        stats[idx].setScore(50);
        ++idx;
      }
      results.setStatus(StatusCode::OK);
    }
    return kj::READY_NOW;
  }

private:
  uint64_t nextTicketId_ = 1;
  uint64_t nextMatchId_ = 1;
  std::map<uint64_t, QueueTicketData> tickets_;
  std::map<uint64_t, std::shared_ptr<MatchData>> matches_;
};

// ---------------------------------------------------------------------------
// Multi-service bootstrap: expose all four services through a single bootstrap
// ---------------------------------------------------------------------------

// We define a simple bootstrap interface that returns each service.
// Since Cap'n Proto RPC bootstraps a single interface, we combine them
// using a struct-like approach: the client calls specific service getters.
//
// However, for simplicity and since the test harness needs flexibility,
// we'll use a "service selector" approach:
// The server accepts a --schema flag to choose which single service to expose
// as the bootstrap interface. The harness starts one server per schema.

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
  std::string host = "127.0.0.1";
  uint16_t port = 0;
  std::string schema = "game_world";

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc) {
      host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      port = static_cast<uint16_t>(std::stoi(argv[++i]));
    } else if (arg == "--schema" && i + 1 < argc) {
      schema = argv[++i];
    }
  }

  try {
    auto io = kj::setupAsyncIo();
    kj::Network& network = io.provider->getNetwork();
    const char* bindHost = (host == "0.0.0.0") ? "*" : host.c_str();
    kj::Own<kj::NetworkAddress> addr = network.parseAddress(bindHost, port).wait(io.waitScope);
    kj::Own<kj::ConnectionReceiver> listener = addr->listen();
    auto boundPort = listener->getPort();

    std::cout << "READY " << boundPort << std::endl;

    if (schema == "game_world") {
      capnp::TwoPartyServer server(kj::heap<GameWorldImpl>());
      server.listen(*listener).wait(io.waitScope);
    } else if (schema == "chat") {
      capnp::TwoPartyServer server(kj::heap<ChatServiceImpl>());
      server.listen(*listener).wait(io.waitScope);
    } else if (schema == "inventory") {
      capnp::TwoPartyServer server(kj::heap<InventoryServiceImpl>());
      server.listen(*listener).wait(io.waitScope);
    } else if (schema == "matchmaking") {
      capnp::TwoPartyServer server(kj::heap<MatchmakingServiceImpl>());
      server.listen(*listener).wait(io.waitScope);
    } else {
      std::cerr << "Unknown schema: " << schema << std::endl;
      std::cerr << "Valid schemas: game_world, chat, inventory, matchmaking" << std::endl;
      return 1;
    }
  } catch (kj::Exception& e) {
    std::cerr << "KJ exception: " << e.getDescription().cStr() << std::endl;
    return 1;
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
