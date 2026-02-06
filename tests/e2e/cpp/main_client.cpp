// C++ reference RPC client for the e2e test suite.
// Connects to the server and runs TAP-format test assertions.

#include <capnp/rpc-twoparty.h>
#include <capnp/message.h>
#include <kj/async-io.h>
#include <kj/debug.h>

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include "game_types.capnp.h"
#include "game_world.capnp.h"
#include "chat.capnp.h"
#include "inventory.capnp.h"
#include "matchmaking.capnp.h"

// ---------------------------------------------------------------------------
// TAP helpers
// ---------------------------------------------------------------------------

static int testNum = 0;
static int failCount = 0;

static void ok(bool pass, const std::string& desc) {
  ++testNum;
  if (pass) {
    std::cout << "ok " << testNum << " - " << desc << std::endl;
  } else {
    std::cout << "not ok " << testNum << " - " << desc << std::endl;
    ++failCount;
  }
}

static void bail(const std::string& msg) {
  std::cout << "Bail out! " << msg << std::endl;
  std::exit(1);
}

static bool approxEq(float a, float b, float eps = 0.01f) {
  return std::fabs(a - b) < eps;
}

// ---------------------------------------------------------------------------
// GameWorld tests
// ---------------------------------------------------------------------------

static void testGameWorld(kj::WaitScope& waitScope, GameWorld::Client client) {
  // Test 1: Spawn an entity
  {
    auto req = client.spawnEntityRequest();
    auto spawn = req.initRequest();
    spawn.setKind(EntityKind::PLAYER);
    spawn.setName("TestHero");
    auto pos = spawn.initPosition();
    pos.setX(10.0f);
    pos.setY(20.0f);
    pos.setZ(30.0f);
    spawn.setFaction(Faction::ALLIANCE);
    spawn.setMaxHealth(200);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "spawnEntity returns OK");
    auto entity = resp.getEntity();
    ok(std::string(entity.getName().cStr()) == "TestHero", "spawned entity has correct name");
    ok(entity.getKind() == EntityKind::PLAYER, "spawned entity has correct kind");
    ok(entity.getHealth() == 200, "spawned entity has full health");
    ok(entity.getMaxHealth() == 200, "spawned entity has correct maxHealth");
    ok(entity.getFaction() == Faction::ALLIANCE, "spawned entity has correct faction");
    ok(entity.getAlive() == true, "spawned entity is alive");
    ok(approxEq(entity.getPosition().getX(), 10.0f), "spawned entity position X");
    ok(approxEq(entity.getPosition().getY(), 20.0f), "spawned entity position Y");
    ok(approxEq(entity.getPosition().getZ(), 30.0f), "spawned entity position Z");
  }

  // Test 2: Get entity
  {
    auto req = client.getEntityRequest();
    req.getId().setId(1);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "getEntity returns OK");
    ok(std::string(resp.getEntity().getName().cStr()) == "TestHero", "getEntity returns correct entity");
  }

  // Test 3: Get non-existent entity
  {
    auto req = client.getEntityRequest();
    req.getId().setId(999);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::NOT_FOUND, "getEntity returns NOT_FOUND for missing entity");
  }

  // Test 4: Move entity
  {
    auto req = client.moveEntityRequest();
    req.getId().setId(1);
    auto newPos = req.initNewPosition();
    newPos.setX(50.0f);
    newPos.setY(60.0f);
    newPos.setZ(70.0f);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "moveEntity returns OK");
    ok(approxEq(resp.getEntity().getPosition().getX(), 50.0f), "entity moved to new X");
    ok(approxEq(resp.getEntity().getPosition().getY(), 60.0f), "entity moved to new Y");
    ok(approxEq(resp.getEntity().getPosition().getZ(), 70.0f), "entity moved to new Z");
  }

  // Test 5: Damage entity (non-lethal)
  {
    auto req = client.damageEntityRequest();
    req.getId().setId(1);
    req.setAmount(50);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "damageEntity returns OK");
    ok(resp.getEntity().getHealth() == 150, "entity health reduced to 150");
    ok(resp.getKilled() == false, "entity not killed by non-lethal damage");
    ok(resp.getEntity().getAlive() == true, "entity still alive");
  }

  // Test 6: Damage entity (lethal)
  {
    auto req = client.damageEntityRequest();
    req.getId().setId(1);
    req.setAmount(999);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "lethal damageEntity returns OK");
    ok(resp.getEntity().getHealth() == 0, "entity health is 0 after lethal damage");
    ok(resp.getKilled() == true, "entity killed by lethal damage");
    ok(resp.getEntity().getAlive() == false, "entity is dead");
  }

  // Test 7: Spawn more entities and query area
  {
    // Spawn entity at origin
    {
      auto req = client.spawnEntityRequest();
      auto spawn = req.initRequest();
      spawn.setKind(EntityKind::NPC);
      spawn.setName("NearNPC");
      auto pos = spawn.initPosition();
      pos.setX(1.0f);
      pos.setY(1.0f);
      pos.setZ(1.0f);
      spawn.setFaction(Faction::NEUTRAL);
      spawn.setMaxHealth(50);
      req.send().wait(waitScope);
    }
    // Spawn entity far away
    {
      auto req = client.spawnEntityRequest();
      auto spawn = req.initRequest();
      spawn.setKind(EntityKind::MONSTER);
      spawn.setName("FarMonster");
      auto pos = spawn.initPosition();
      pos.setX(1000.0f);
      pos.setY(1000.0f);
      pos.setZ(1000.0f);
      spawn.setFaction(Faction::HORDE);
      spawn.setMaxHealth(300);
      req.send().wait(waitScope);
    }

    // Query area around origin with large radius - should find the NPC and
    // the dead player (who was moved to 50,60,70 earlier)
    {
      auto req = client.queryAreaRequest();
      auto query = req.initQuery();
      auto center = query.initCenter();
      center.setX(0.0f);
      center.setY(0.0f);
      center.setZ(0.0f);
      query.setRadius(100.0f);
      query.getFilter().setAll();
      auto resp = req.send().wait(waitScope);
      ok(resp.getCount() >= 1, "queryArea finds at least 1 entity near origin");
    }

    // Query with faction filter
    {
      auto req = client.queryAreaRequest();
      auto query = req.initQuery();
      auto center = query.initCenter();
      center.setX(0.0f);
      center.setY(0.0f);
      center.setZ(0.0f);
      query.setRadius(100.0f);
      query.getFilter().setByFaction(Faction::NEUTRAL);
      auto resp = req.send().wait(waitScope);
      ok(resp.getCount() >= 1, "queryArea with faction filter finds neutral NPC");
    }

    // Query with kind filter
    {
      auto req = client.queryAreaRequest();
      auto query = req.initQuery();
      auto center = query.initCenter();
      center.setX(0.0f);
      center.setY(0.0f);
      center.setZ(0.0f);
      query.setRadius(100.0f);
      query.getFilter().setByKind(EntityKind::NPC);
      auto resp = req.send().wait(waitScope);
      ok(resp.getCount() >= 1, "queryArea with kind filter finds NPC");
    }
  }

  // Test 8: Despawn entity
  {
    auto req = client.despawnEntityRequest();
    req.getId().setId(1);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "despawnEntity returns OK");
  }

  // Test 9: Despawn non-existent entity
  {
    auto req = client.despawnEntityRequest();
    req.getId().setId(999);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::NOT_FOUND, "despawnEntity returns NOT_FOUND for missing entity");
  }
}

// ---------------------------------------------------------------------------
// Chat tests
// ---------------------------------------------------------------------------

static void testChat(kj::WaitScope& waitScope, ChatService::Client client) {
  // Test 1: Create a room
  ChatRoom::Client room = nullptr;
  {
    auto req = client.createRoomRequest();
    req.setName("general");
    req.setTopic("General chat for all players");
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "createRoom returns OK");
    auto info = resp.getInfo();
    ok(std::string(info.getName().cStr()) == "general", "created room has correct name");
    ok(std::string(info.getTopic().cStr()) == "General chat for all players", "created room has correct topic");
    room = resp.getRoom();
  }

  // Test 2: Join the room
  ChatRoom::Client joinedRoom = nullptr;
  {
    auto req = client.joinRoomRequest();
    req.setName("general");
    auto player = req.initPlayer();
    player.getId().setId(42);
    player.setName("PlayerOne");
    player.setFaction(Faction::ALLIANCE);
    player.setLevel(60);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "joinRoom returns OK");
    joinedRoom = resp.getRoom();
  }

  // Test 3: Send a message via the joined room capability
  {
    auto req = joinedRoom.sendMessageRequest();
    req.setContent("Hello, world!");
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "sendMessage returns OK");
    auto msg = resp.getMessage();
    ok(std::string(msg.getContent().cStr()) == "Hello, world!", "message content matches");
    ok(std::string(msg.getSender().getName().cStr()) == "PlayerOne", "message sender name matches");
    ok(msg.getSender().getFaction() == Faction::ALLIANCE, "message sender faction matches");
    ok(msg.getKind().isNormal(), "message kind is normal");
  }

  // Test 4: Send an emote
  {
    auto req = joinedRoom.sendEmoteRequest();
    req.setContent("dances");
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "sendEmote returns OK");
    ok(resp.getMessage().getKind().isEmote(), "emote message kind is emote");
    ok(std::string(resp.getMessage().getContent().cStr()) == "dances", "emote content matches");
  }

  // Test 5: Get history
  {
    auto req = joinedRoom.getHistoryRequest();
    req.setLimit(10);
    auto resp = req.send().wait(waitScope);
    ok(resp.getMessages().size() >= 2, "getHistory returns at least 2 messages");
  }

  // Test 6: Get room info
  {
    auto req = joinedRoom.getInfoRequest();
    auto resp = req.send().wait(waitScope);
    auto info = resp.getInfo();
    ok(std::string(info.getName().cStr()) == "general", "getInfo returns correct room name");
    ok(std::string(info.getTopic().cStr()) == "General chat for all players", "getInfo returns correct topic");
  }

  // Test 7: List rooms
  {
    auto req = client.listRoomsRequest();
    auto resp = req.send().wait(waitScope);
    ok(resp.getRooms().size() >= 1, "listRooms returns at least 1 room");
    bool found = false;
    for (auto r : resp.getRooms()) {
      if (std::string(r.getName().cStr()) == "general") found = true;
    }
    ok(found, "listRooms includes the 'general' room");
  }

  // Test 8: Whisper
  {
    auto req = client.whisperRequest();
    auto from = req.initFrom();
    from.getId().setId(42);
    from.setName("PlayerOne");
    from.setFaction(Faction::ALLIANCE);
    from.setLevel(60);
    req.getTo().setId(99);
    req.setContent("secret message");
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "whisper returns OK");
    ok(resp.getMessage().getKind().isWhisper(), "whisper message kind is whisper");
    ok(resp.getMessage().getKind().getWhisper().getId() == 99, "whisper target ID matches");
    ok(std::string(resp.getMessage().getContent().cStr()) == "secret message", "whisper content matches");
  }

  // Test 9: Join non-existent room
  {
    auto req = client.joinRoomRequest();
    req.setName("nonexistent");
    auto player = req.initPlayer();
    player.getId().setId(1);
    player.setName("Nobody");
    player.setFaction(Faction::NEUTRAL);
    player.setLevel(1);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::NOT_FOUND, "joinRoom returns NOT_FOUND for nonexistent room");
  }

  // Test 10: Leave room
  {
    auto req = joinedRoom.leaveRequest();
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "leave room returns OK");
  }
}

// ---------------------------------------------------------------------------
// Inventory tests
// ---------------------------------------------------------------------------

static void testInventory(kj::WaitScope& waitScope, InventoryService::Client client) {
  uint64_t playerId = 42;

  // Test 1: Get empty inventory
  {
    auto req = client.getInventoryRequest();
    req.getPlayer().setId(playerId);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "getInventory returns OK for new player");
    ok(resp.getInventory().getUsedSlots() == 0, "new player inventory has 0 used slots");
    ok(resp.getInventory().getCapacity() == 20, "new player inventory has capacity 20");
  }

  // Test 2: Add items
  {
    auto req = client.addItemRequest();
    req.getPlayer().setId(playerId);
    auto item = req.initItem();
    item.getId().setId(100);
    item.setName("Iron Sword");
    item.setRarity(Rarity::COMMON);
    item.setLevel(10);
    item.setStackSize(1);
    auto attrs = item.initAttributes(2);
    attrs[0].setName("attack");
    attrs[0].setValue(25);
    attrs[1].setName("durability");
    attrs[1].setValue(100);
    req.setQuantity(1);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "addItem returns OK");
    ok(resp.getSlot().getSlotIndex() == 0, "first item goes to slot 0");
    ok(std::string(resp.getSlot().getItem().getName().cStr()) == "Iron Sword", "added item name matches");
    ok(resp.getSlot().getItem().getRarity() == Rarity::COMMON, "added item rarity matches");
    ok(resp.getSlot().getItem().getAttributes().size() == 2, "added item has 2 attributes");
  }

  // Test 3: Add a rare item
  {
    auto req = client.addItemRequest();
    req.getPlayer().setId(playerId);
    auto item = req.initItem();
    item.getId().setId(200);
    item.setName("Dragon Scale Shield");
    item.setRarity(Rarity::EPIC);
    item.setLevel(50);
    item.setStackSize(1);
    auto attrs = item.initAttributes(1);
    attrs[0].setName("defense");
    attrs[0].setValue(80);
    req.setQuantity(1);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "addItem (epic) returns OK");
    ok(resp.getSlot().getSlotIndex() == 1, "second item goes to slot 1");
  }

  // Test 4: Add a stack of potions
  {
    auto req = client.addItemRequest();
    req.getPlayer().setId(playerId);
    auto item = req.initItem();
    item.getId().setId(300);
    item.setName("Health Potion");
    item.setRarity(Rarity::COMMON);
    item.setLevel(1);
    item.setStackSize(20);
    item.initAttributes(0);
    req.setQuantity(5);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "addItem (stackable) returns OK");
    ok(resp.getSlot().getQuantity() == 5, "stackable item quantity is 5");
  }

  // Test 5: Get inventory with items
  {
    auto req = client.getInventoryRequest();
    req.getPlayer().setId(playerId);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "getInventory returns OK after adding items");
    ok(resp.getInventory().getUsedSlots() == 3, "inventory has 3 used slots");
    ok(resp.getInventory().getSlots().size() == 3, "inventory has 3 slot entries");
  }

  // Test 6: Filter by rarity
  {
    auto req = client.filterByRarityRequest();
    req.getPlayer().setId(playerId);
    req.setMinRarity(Rarity::EPIC);
    auto resp = req.send().wait(waitScope);
    ok(resp.getItems().size() == 1, "filterByRarity(epic+) returns 1 item");
    ok(std::string(resp.getItems()[0].getItem().getName().cStr()) == "Dragon Scale Shield",
       "filtered item is Dragon Scale Shield");
  }

  // Test 7: Filter by rarity (common+)
  {
    auto req = client.filterByRarityRequest();
    req.getPlayer().setId(playerId);
    req.setMinRarity(Rarity::COMMON);
    auto resp = req.send().wait(waitScope);
    ok(resp.getItems().size() == 3, "filterByRarity(common+) returns all 3 items");
  }

  // Test 8: Remove item
  {
    auto req = client.removeItemRequest();
    req.getPlayer().setId(playerId);
    req.setSlotIndex(2);
    req.setQuantity(3);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "removeItem (partial) returns OK");
  }

  // Test 9: Verify partial removal
  {
    auto req = client.getInventoryRequest();
    req.getPlayer().setId(playerId);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "getInventory after partial remove returns OK");
    // Find slot 2 and check quantity
    bool found = false;
    for (auto slot : resp.getInventory().getSlots()) {
      if (slot.getSlotIndex() == 2) {
        ok(slot.getQuantity() == 2, "partial remove: quantity reduced to 2");
        found = true;
      }
    }
    ok(found, "slot 2 still exists after partial remove");
  }

  // Test 10: Start a trade session
  {
    auto req = client.startTradeRequest();
    req.getInitiator().setId(playerId);
    req.getTarget().setId(99);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "startTrade returns OK");
    auto session = resp.getSession();

    // Test 11: Get trade state
    {
      auto stateReq = session.getStateRequest();
      auto stateResp = stateReq.send().wait(waitScope);
      ok(stateResp.getState() == TradeState::PROPOSING, "initial trade state is PROPOSING");
    }

    // Test 12: Offer items
    {
      auto offerReq = session.offerItemsRequest();
      auto slots = offerReq.initSlots(1);
      slots.set(0, 0);
      auto offerResp = offerReq.send().wait(waitScope);
      ok(offerResp.getStatus() == StatusCode::OK, "offerItems returns OK");
      ok(offerResp.getOffer().getOfferedItems().size() == 1, "offer contains 1 item");
    }

    // Test 13: Accept trade
    {
      auto acceptReq = session.acceptRequest();
      auto acceptResp = acceptReq.send().wait(waitScope);
      ok(acceptResp.getStatus() == StatusCode::OK, "accept returns OK");
    }

    // Test 14: Cancel trade
    {
      auto cancelReq = session.cancelRequest();
      auto cancelResp = cancelReq.send().wait(waitScope);
      ok(cancelResp.getState() == TradeState::CANCELLED, "cancel returns CANCELLED state");
    }
  }
}

// ---------------------------------------------------------------------------
// Matchmaking tests
// ---------------------------------------------------------------------------

static void testMatchmaking(kj::WaitScope& waitScope, MatchmakingService::Client client) {
  // Test 1: Enqueue a player
  uint64_t ticketId = 0;
  {
    auto req = client.enqueueRequest();
    auto player = req.initPlayer();
    player.getId().setId(42);
    player.setName("TestPlayer");
    player.setFaction(Faction::ALLIANCE);
    player.setLevel(60);
    req.setMode(GameMode::DUEL);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "enqueue returns OK");
    auto ticket = resp.getTicket();
    ok(ticket.getTicketId() > 0, "ticket has non-zero ID");
    ok(std::string(ticket.getPlayer().getName().cStr()) == "TestPlayer", "ticket player name matches");
    ok(ticket.getMode() == GameMode::DUEL, "ticket mode matches");
    ok(ticket.getEstimatedWaitSecs() > 0, "ticket has estimated wait time");
    ticketId = ticket.getTicketId();
  }

  // Test 2: Get queue stats
  {
    auto req = client.getQueueStatsRequest();
    req.setMode(GameMode::DUEL);
    auto resp = req.send().wait(waitScope);
    ok(resp.getPlayersInQueue() >= 1, "queue has at least 1 player in duel mode");
  }

  // Test 3: Dequeue
  {
    auto req = client.dequeueRequest();
    req.setTicketId(ticketId);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "dequeue returns OK");
  }

  // Test 4: Dequeue non-existent ticket
  {
    auto req = client.dequeueRequest();
    req.setTicketId(99999);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::NOT_FOUND, "dequeue returns NOT_FOUND for invalid ticket");
  }

  // Test 5: Queue stats after dequeue
  {
    auto req = client.getQueueStatsRequest();
    req.setMode(GameMode::DUEL);
    auto resp = req.send().wait(waitScope);
    ok(resp.getPlayersInQueue() == 0, "queue empty after dequeue");
  }

  // Test 6: Find match (returns MatchController capability)
  MatchController::Client controller = nullptr;
  uint64_t matchId = 0;
  {
    auto req = client.findMatchRequest();
    auto player = req.initPlayer();
    player.getId().setId(42);
    player.setName("TestPlayer");
    player.setFaction(Faction::ALLIANCE);
    player.setLevel(60);
    req.setMode(GameMode::ARENA3V3);
    auto resp = req.send().wait(waitScope);
    matchId = resp.getMatchId().getId();
    ok(matchId > 0, "findMatch returns non-zero matchId");
    controller = resp.getController();
  }

  // Test 7: Get match info via MatchController capability
  {
    auto req = controller.getInfoRequest();
    auto resp = req.send().wait(waitScope);
    auto info = resp.getInfo();
    ok(info.getId().getId() == matchId, "getInfo matchId matches");
    ok(info.getMode() == GameMode::ARENA3V3, "getInfo mode is ARENA3V3");
    ok(info.getState() == MatchState::WAITING, "match starts in WAITING state");
    ok(info.getTeamA().size() >= 1, "teamA has at least 1 player");
    ok(info.getTeamB().size() >= 1, "teamB has at least 1 player (bot)");
  }

  // Test 8: Signal ready via MatchController
  {
    auto req = controller.signalReadyRequest();
    req.getPlayer().setId(42);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "signalReady returns OK");
    ok(resp.getAllReady() == true, "all players ready");
  }

  // Test 9: Get match info after ready (should be READY state)
  {
    auto req = controller.getInfoRequest();
    auto resp = req.send().wait(waitScope);
    ok(resp.getInfo().getState() == MatchState::READY, "match state is READY after signalReady");
  }

  // Test 10: Report match result
  {
    auto req = controller.reportResultRequest();
    auto result = req.initResult();
    result.getMatchId().setId(matchId);
    result.setWinningTeam(0);
    result.setDuration(180);
    auto stats = result.initPlayerStats(1);
    auto ps = stats[0];
    ps.initPlayer().getId().setId(42);
    ps.getPlayer().setName("TestPlayer");
    ps.getPlayer().setFaction(Faction::ALLIANCE);
    ps.getPlayer().setLevel(60);
    ps.setKills(10);
    ps.setDeaths(3);
    ps.setAssists(5);
    ps.setScore(200);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "reportResult returns OK");
  }

  // Test 11: Get match result via service
  {
    auto req = client.getMatchResultRequest();
    req.getId().setId(matchId);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::OK, "getMatchResult returns OK");
    ok(resp.getResult().getMatchId().getId() == matchId, "getMatchResult matchId matches");
    ok(resp.getResult().getWinningTeam() == 0, "winning team is teamA");
    ok(resp.getResult().getPlayerStats().size() >= 1, "match result has player stats");
  }

  // Test 12: Get result for non-existent match
  {
    auto req = client.getMatchResultRequest();
    req.getId().setId(99999);
    auto resp = req.send().wait(waitScope);
    ok(resp.getStatus() == StatusCode::NOT_FOUND, "getMatchResult returns NOT_FOUND for invalid match");
  }

  // Test 13: Promise pipelining - call getInfo on controller from findMatch
  // before the findMatch promise resolves. In practice Cap'n Proto
  // automatically pipelines, but we demonstrate it explicitly here.
  {
    auto findReq = client.findMatchRequest();
    auto player = findReq.initPlayer();
    player.getId().setId(77);
    player.setName("PipelinePlayer");
    player.setFaction(Faction::HORDE);
    player.setLevel(45);
    findReq.setMode(GameMode::BATTLEGROUND);

    // Send findMatch but don't wait - immediately pipeline a getInfo call
    auto findPromise = findReq.send();
    auto pipelinedController = findPromise.getController();
    auto infoReq = pipelinedController.getInfoRequest();
    auto infoResp = infoReq.send().wait(waitScope);

    ok(infoResp.getInfo().getMode() == GameMode::BATTLEGROUND,
       "pipelined getInfo returns correct mode");
    ok(infoResp.getInfo().getTeamA().size() >= 1,
       "pipelined getInfo returns teamA");
  }

  // Test 14: Cancel match
  {
    // Create a new match to cancel
    auto findReq = client.findMatchRequest();
    auto player = findReq.initPlayer();
    player.getId().setId(88);
    player.setName("CancelPlayer");
    player.setFaction(Faction::NEUTRAL);
    player.setLevel(10);
    findReq.setMode(GameMode::DUEL);
    auto findResp = findReq.send().wait(waitScope);
    auto cancelController = findResp.getController();

    auto cancelReq = cancelController.cancelMatchRequest();
    auto cancelResp = cancelReq.send().wait(waitScope);
    ok(cancelResp.getStatus() == StatusCode::OK, "cancelMatch returns OK for waiting match");

    // Verify cancelled state
    auto infoReq = cancelController.getInfoRequest();
    auto infoResp = infoReq.send().wait(waitScope);
    ok(infoResp.getInfo().getState() == MatchState::CANCELLED,
       "match state is CANCELLED after cancelMatch");
  }
}

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

  if (port == 0) {
    std::cerr << "Error: --port is required" << std::endl;
    return 1;
  }

  try {
    auto io = kj::setupAsyncIo();
    auto& waitScope = io.waitScope;

    kj::Network& network = io.provider->getNetwork();
    kj::Own<kj::NetworkAddress> addr = network.parseAddress(host.c_str(), port).wait(waitScope);
    kj::Own<kj::AsyncIoStream> conn = addr->connect().wait(waitScope);
    capnp::TwoPartyClient rpcClient(*conn);

    if (schema == "game_world") {
      auto client = rpcClient.bootstrap().castAs<GameWorld>();
      testGameWorld(waitScope, client);
    } else if (schema == "chat") {
      auto client = rpcClient.bootstrap().castAs<ChatService>();
      testChat(waitScope, client);
    } else if (schema == "inventory") {
      auto client = rpcClient.bootstrap().castAs<InventoryService>();
      testInventory(waitScope, client);
    } else if (schema == "matchmaking") {
      auto client = rpcClient.bootstrap().castAs<MatchmakingService>();
      testMatchmaking(waitScope, client);
    } else {
      std::cerr << "Unknown schema: " << schema << std::endl;
      return 1;
    }
  } catch (kj::Exception& e) {
    bail(std::string("KJ exception: ") + e.getDescription().cStr());
  } catch (std::exception& e) {
    bail(std::string("Exception: ") + e.what());
  }

  std::cout << "1.." << testNum << std::endl;
  return failCount > 0 ? 1 : 0;
}
