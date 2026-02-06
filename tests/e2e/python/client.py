#!/usr/bin/env python3
"""Cap'n Proto RPC e2e test client -- Python (pycapnp) implementation.

Connects to an RPC server and exercises all four service interfaces,
printing results in TAP (Test Anything Protocol) format.

Each service is expected on a consecutive port starting from --port:
  GameWorld:          port + 0
  ChatService:        port + 1
  InventoryService:   port + 2
  MatchmakingService: port + 3

Usage:
  python client.py --host localhost --port 4003
"""

import argparse
import asyncio
import inspect
import os
import sys
import traceback

import capnp

# ---------------------------------------------------------------------------
# Load schemas
# ---------------------------------------------------------------------------

SCHEMA_DIR = os.environ.get(
    "SCHEMA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "schemas"),
)

SCHEMA_PARSER = capnp.SchemaParser()

game_world_capnp = SCHEMA_PARSER.load(
    os.path.join(SCHEMA_DIR, "game_world.capnp"),
    imports=[SCHEMA_DIR],
)
chat_capnp = SCHEMA_PARSER.load(
    os.path.join(SCHEMA_DIR, "chat.capnp"),
    imports=[SCHEMA_DIR],
)
inventory_capnp = SCHEMA_PARSER.load(
    os.path.join(SCHEMA_DIR, "inventory.capnp"),
    imports=[SCHEMA_DIR],
)
matchmaking_capnp = SCHEMA_PARSER.load(
    os.path.join(SCHEMA_DIR, "matchmaking.capnp"),
    imports=[SCHEMA_DIR],
)


# ---------------------------------------------------------------------------
# TAP helpers
# ---------------------------------------------------------------------------

class TapReporter:
    def __init__(self):
        self._count = 0
        self._failures = 0

    def ok(self, desc):
        self._count += 1
        print(f"ok {self._count} - {desc}")

    def not_ok(self, desc, detail=""):
        self._count += 1
        self._failures += 1
        print(f"not ok {self._count} - {desc}")
        if detail:
            for line in detail.strip().splitlines():
                print(f"  # {line}")

    def test(self, desc, condition, detail=""):
        if condition:
            self.ok(desc)
        else:
            self.not_ok(desc, detail)

    def done(self):
        print(f"1..{self._count}")
        return self._failures == 0


tap = TapReporter()


# ---------------------------------------------------------------------------
# Connect helper
# ---------------------------------------------------------------------------

async def connect(host, port, interface):
    conn = await capnp.AsyncIoStream.create_connection(host=host, port=str(port))
    client = capnp.TwoPartyClient(conn)
    cap = client.bootstrap().cast_as(interface)
    return cap, client


async def await_response(req_or_promise):
    obj = req_or_promise
    # Avoid probing instance attributes on pycapnp dynamic structs: unknown attribute
    # access may abort at the C++ layer instead of raising AttributeError.
    send_fn = getattr(type(obj), "send", None)
    if send_fn is not None:
        obj = send_fn(obj)

    if inspect.isawaitable(obj):
        return await obj

    a_wait_fn = getattr(type(obj), "a_wait", None)
    if a_wait_fn is not None:
        return await a_wait_fn(obj)

    return obj


# ===========================================================================
# GameWorld tests
# ===========================================================================

async def test_game_world(host, port):
    gw, client = await connect(host, port, game_world_capnp.GameWorld)

    # -- spawnEntity --------------------------------------------------------
    try:
        resp = await gw.spawnEntity(
            request={
                "kind": "player",
                "name": "TestHero",
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "faction": "alliance",
                "maxHealth": 100,
            }
        )
        tap.test(
            "GameWorld.spawnEntity returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "GameWorld.spawnEntity returns entity with correct name",
            resp.entity.name == "TestHero",
            f"name={resp.entity.name}",
        )
        tap.test(
            "GameWorld.spawnEntity returns entity with full health",
            resp.entity.health == 100,
            f"health={resp.entity.health}",
        )
        entity_id = resp.entity.id.id
    except Exception as e:
        tap.not_ok("GameWorld.spawnEntity returns ok", traceback.format_exc())
        tap.not_ok("GameWorld.spawnEntity returns entity with correct name", "skipped")
        tap.not_ok("GameWorld.spawnEntity returns entity with full health", "skipped")
        return

    # -- getEntity ----------------------------------------------------------
    try:
        resp = await gw.getEntity(id={"id": entity_id})
        tap.test(
            "GameWorld.getEntity retrieves spawned entity",
            str(resp.status) == "ok" and resp.entity.name == "TestHero",
            f"status={resp.status}, name={resp.entity.name}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.getEntity retrieves spawned entity", traceback.format_exc())

    # -- moveEntity ---------------------------------------------------------
    try:
        resp = await gw.moveEntity(id={"id": entity_id}, newPosition={"x": 10.0, "y": 20.0, "z": 30.0})
        tap.test(
            "GameWorld.moveEntity updates position",
            str(resp.status) == "ok" and abs(resp.entity.position.x - 10.0) < 0.01,
            f"status={resp.status}, x={resp.entity.position.x}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.moveEntity updates position", traceback.format_exc())

    # -- damageEntity -------------------------------------------------------
    try:
        resp = await gw.damageEntity(id={"id": entity_id}, amount=30)
        tap.test(
            "GameWorld.damageEntity reduces health",
            str(resp.status) == "ok" and resp.entity.health == 70,
            f"status={resp.status}, health={resp.entity.health}",
        )
        tap.test(
            "GameWorld.damageEntity not killed at 70 HP",
            not resp.killed,
            f"killed={resp.killed}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.damageEntity reduces health", traceback.format_exc())
        tap.not_ok("GameWorld.damageEntity not killed at 70 HP", "skipped")

    # -- damageEntity (lethal) ----------------------------------------------
    try:
        resp = await gw.damageEntity(id={"id": entity_id}, amount=200)
        tap.test(
            "GameWorld.damageEntity kills at 0 HP",
            resp.killed and resp.entity.health == 0,
            f"killed={resp.killed}, health={resp.entity.health}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.damageEntity kills at 0 HP", traceback.format_exc())

    # -- Spawn more entities for queryArea ----------------------------------
    try:
        await gw.spawnEntity(request={
            "kind": "npc",
            "name": "FriendlyNPC",
            "position": {"x": 5.0, "y": 5.0, "z": 5.0},
            "faction": "alliance",
            "maxHealth": 50,
        })
        await gw.spawnEntity(request={
            "kind": "monster",
            "name": "EvilDragon",
            "position": {"x": 100.0, "y": 100.0, "z": 100.0},
            "faction": "horde",
            "maxHealth": 500,
        })
    except Exception:
        pass

    # -- queryArea (all) ----------------------------------------------------
    try:
        resp = await gw.queryArea(query={
            "center": {"x": 5.0, "y": 5.0, "z": 5.0},
            "radius": 50.0,
            "filter": {"all": None},
        })
        tap.test(
            "GameWorld.queryArea returns entities within radius",
            resp.count >= 1,
            f"count={resp.count}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.queryArea returns entities within radius", traceback.format_exc())

    # -- queryArea (byKind filter) ------------------------------------------
    try:
        resp = await gw.queryArea(query={
            "center": {"x": 5.0, "y": 5.0, "z": 5.0},
            "radius": 50.0,
            "filter": {"byKind": "npc"},
        })
        all_npc = all(str(e.kind) == "npc" for e in resp.entities)
        tap.test(
            "GameWorld.queryArea byKind filter returns only NPCs",
            resp.count >= 1 and all_npc,
            f"count={resp.count}, allNpc={all_npc}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.queryArea byKind filter returns only NPCs", traceback.format_exc())

    # -- despawnEntity ------------------------------------------------------
    try:
        resp = await gw.despawnEntity(id={"id": entity_id})
        tap.test(
            "GameWorld.despawnEntity returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.despawnEntity returns ok", traceback.format_exc())

    # -- getEntity after despawn (should be notFound) -----------------------
    try:
        resp = await gw.getEntity(id={"id": entity_id})
        tap.test(
            "GameWorld.getEntity returns notFound after despawn",
            str(resp.status) == "notFound",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("GameWorld.getEntity returns notFound after despawn", traceback.format_exc())


# ===========================================================================
# ChatService tests
# ===========================================================================

async def test_chat_service(host, port):
    cs, client = await connect(host, port, chat_capnp.ChatService)

    # -- createRoom ---------------------------------------------------------
    try:
        # pycapnp RPC helper uses "name" as an internal kwarg; use request builder
        # for methods that also have a "name" field in params.
        create_req = cs.createRoom_request()
        create_req.name = "general"
        create_req.topic = "General discussion"
        resp = await await_response(create_req)
        tap.test(
            "ChatService.createRoom returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "ChatService.createRoom returns room info",
            resp.info.name == "general" and resp.info.topic == "General discussion",
            f"name={resp.info.name}, topic={resp.info.topic}",
        )
        room = resp.room
    except Exception as e:
        tap.not_ok("ChatService.createRoom returns ok", traceback.format_exc())
        tap.not_ok("ChatService.createRoom returns room info", "skipped")
        return

    # -- joinRoom -----------------------------------------------------------
    try:
        join_req = cs.joinRoom_request()
        join_req.name = "general"
        join_req.player.id.id = 42
        join_req.player.name = "TestPlayer"
        join_req.player.faction = "alliance"
        join_req.player.level = 10
        resp = await await_response(join_req)
        tap.test(
            "ChatService.joinRoom returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        room = resp.room
    except Exception as e:
        tap.not_ok("ChatService.joinRoom returns ok", traceback.format_exc())
        return

    # -- ChatRoom.sendMessage (capability passing test) ---------------------
    try:
        resp = await room.sendMessage(content="Hello, world!")
        tap.test(
            "ChatRoom.sendMessage via capability returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "ChatRoom.sendMessage content matches",
            resp.message.content == "Hello, world!",
            f"content={resp.message.content}",
        )
        tap.test(
            "ChatRoom.sendMessage kind is normal",
            resp.message.kind.which() == "normal",
            f"kind={resp.message.kind.which()}",
        )
    except Exception as e:
        tap.not_ok("ChatRoom.sendMessage via capability returns ok", traceback.format_exc())
        tap.not_ok("ChatRoom.sendMessage content matches", "skipped")
        tap.not_ok("ChatRoom.sendMessage kind is normal", "skipped")

    # -- ChatRoom.sendEmote -------------------------------------------------
    try:
        resp = await room.sendEmote(content="dances")
        tap.test(
            "ChatRoom.sendEmote kind is emote",
            resp.message.kind.which() == "emote",
            f"kind={resp.message.kind.which()}",
        )
    except Exception as e:
        tap.not_ok("ChatRoom.sendEmote kind is emote", traceback.format_exc())

    # -- ChatRoom.getHistory ------------------------------------------------
    try:
        resp = await room.getHistory(limit=10)
        tap.test(
            "ChatRoom.getHistory returns sent messages",
            len(resp.messages) >= 2,
            f"count={len(resp.messages)}",
        )
    except Exception as e:
        tap.not_ok("ChatRoom.getHistory returns sent messages", traceback.format_exc())

    # -- ChatRoom.getInfo ---------------------------------------------------
    try:
        resp = await room.getInfo()
        tap.test(
            "ChatRoom.getInfo returns room name",
            resp.info.name == "general",
            f"name={resp.info.name}",
        )
    except Exception as e:
        tap.not_ok("ChatRoom.getInfo returns room name", traceback.format_exc())

    # -- listRooms ----------------------------------------------------------
    try:
        resp = await cs.listRooms()
        tap.test(
            "ChatService.listRooms includes created room",
            len(resp.rooms) >= 1 and resp.rooms[0].name == "general",
            f"count={len(resp.rooms)}",
        )
    except Exception as e:
        tap.not_ok("ChatService.listRooms includes created room", traceback.format_exc())

    # -- whisper ------------------------------------------------------------
    try:
        whisper_req = cs.whisper_request(
            **{
                "from": {
                    "id": {"id": 42},
                    "name": "TestPlayer",
                    "faction": "alliance",
                    "level": 10,
                },
                "to": {"id": 99},
                "content": "secret message",
            }
        )
        resp = await await_response(whisper_req)
        tap.test(
            "ChatService.whisper returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "ChatService.whisper kind is whisper",
            resp.message.kind.which() == "whisper",
            f"kind={resp.message.kind.which()}",
        )
    except Exception as e:
        # Try alternate approach if request_ style doesn't work
        try:
            resp = await cs.whisper(
                **{
                    "from": {
                        "id": {"id": 42},
                        "name": "TestPlayer",
                        "faction": "alliance",
                        "level": 10,
                    },
                    "to": {"id": 99},
                    "content": "secret message",
                }
            )
            tap.test(
                "ChatService.whisper returns ok",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
            tap.test(
                "ChatService.whisper kind is whisper",
                resp.message.kind.which() == "whisper",
                f"kind={resp.message.kind.which()}",
            )
        except Exception as e2:
            tap.not_ok("ChatService.whisper returns ok", traceback.format_exc())
            tap.not_ok("ChatService.whisper kind is whisper", "skipped")

    # -- ChatRoom.leave -----------------------------------------------------
    try:
        resp = await room.leave()
        tap.test(
            "ChatRoom.leave returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("ChatRoom.leave returns ok", traceback.format_exc())

    # -- joinRoom for non-existent room ------------------------------------
    try:
        missing_join_req = cs.joinRoom_request()
        missing_join_req.name = "nonexistent"
        missing_join_req.player.id.id = 42
        missing_join_req.player.name = "TestPlayer"
        missing_join_req.player.faction = "alliance"
        missing_join_req.player.level = 10
        resp = await await_response(missing_join_req)
        tap.test(
            "ChatService.joinRoom returns notFound for unknown room",
            str(resp.status) == "notFound",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("ChatService.joinRoom returns notFound for unknown room", traceback.format_exc())


# ===========================================================================
# InventoryService tests
# ===========================================================================

async def test_inventory_service(host, port):
    inv, client = await connect(host, port, inventory_capnp.InventoryService)

    player_id = {"id": 42}

    # -- getInventory (empty) -----------------------------------------------
    try:
        resp = await inv.getInventory(player=player_id)
        tap.test(
            "InventoryService.getInventory returns ok for new player",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "InventoryService.getInventory starts empty",
            resp.inventory.usedSlots == 0,
            f"usedSlots={resp.inventory.usedSlots}",
        )
    except Exception as e:
        tap.not_ok("InventoryService.getInventory returns ok for new player", traceback.format_exc())
        tap.not_ok("InventoryService.getInventory starts empty", "skipped")

    # -- addItem ------------------------------------------------------------
    try:
        resp = await inv.addItem(
            player=player_id,
            item={
                "id": {"id": 1001},
                "name": "Sword of Testing",
                "rarity": "epic",
                "level": 60,
                "stackSize": 1,
                "attributes": [
                    {"name": "strength", "value": 42},
                    {"name": "stamina", "value": 15},
                ],
            },
            quantity=1,
        )
        tap.test(
            "InventoryService.addItem returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "InventoryService.addItem returns correct item name",
            resp.slot.item.name == "Sword of Testing",
            f"name={resp.slot.item.name}",
        )
        sword_slot = resp.slot.slotIndex
    except Exception as e:
        tap.not_ok("InventoryService.addItem returns ok", traceback.format_exc())
        tap.not_ok("InventoryService.addItem returns correct item name", "skipped")
        sword_slot = 0

    # Add another item (common)
    try:
        resp = await inv.addItem(
            player=player_id,
            item={
                "id": {"id": 1002},
                "name": "Health Potion",
                "rarity": "common",
                "level": 1,
                "stackSize": 20,
                "attributes": [],
            },
            quantity=5,
        )
        potion_slot = resp.slot.slotIndex
    except Exception:
        potion_slot = 1

    # -- getInventory (after adds) ------------------------------------------
    try:
        resp = await inv.getInventory(player=player_id)
        tap.test(
            "InventoryService.getInventory shows added items",
            resp.inventory.usedSlots == 2,
            f"usedSlots={resp.inventory.usedSlots}",
        )
    except Exception as e:
        tap.not_ok("InventoryService.getInventory shows added items", traceback.format_exc())

    # -- filterByRarity -----------------------------------------------------
    try:
        resp = await inv.filterByRarity(player=player_id, minRarity="rare")
        tap.test(
            "InventoryService.filterByRarity returns only epic+ items",
            len(resp.items) == 1 and resp.items[0].item.name == "Sword of Testing",
            f"count={len(resp.items)}",
        )
    except Exception as e:
        tap.not_ok("InventoryService.filterByRarity returns only epic+ items", traceback.format_exc())

    # -- removeItem ---------------------------------------------------------
    try:
        resp = await inv.removeItem(player=player_id, slotIndex=potion_slot, quantity=5)
        tap.test(
            "InventoryService.removeItem returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("InventoryService.removeItem returns ok", traceback.format_exc())

    # -- startTrade (capability passing) ------------------------------------
    try:
        resp = await inv.startTrade(
            initiator=player_id,
            target={"id": 99},
        )
        tap.test(
            "InventoryService.startTrade returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        trade = resp.session
    except Exception as e:
        tap.not_ok("InventoryService.startTrade returns ok", traceback.format_exc())
        trade = None

    if trade:
        # -- TradeSession.offerItems ----------------------------------------
        try:
            resp = await trade.offerItems(slots=[sword_slot])
            tap.test(
                "TradeSession.offerItems returns ok",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
            tap.test(
                "TradeSession.offerItems shows offered slot",
                len(resp.offer.offeredItems) == 1,
                f"count={len(resp.offer.offeredItems)}",
            )
        except Exception as e:
            tap.not_ok("TradeSession.offerItems returns ok", traceback.format_exc())
            tap.not_ok("TradeSession.offerItems shows offered slot", "skipped")

        # -- TradeSession.getState ------------------------------------------
        try:
            resp = await trade.getState()
            tap.test(
                "TradeSession.getState returns proposing",
                str(resp.state) == "proposing",
                f"state={resp.state}",
            )
        except Exception as e:
            tap.not_ok("TradeSession.getState returns proposing", traceback.format_exc())

        # -- TradeSession.accept --------------------------------------------
        try:
            resp = await trade.accept()
            tap.test(
                "TradeSession.accept returns ok",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
        except Exception as e:
            tap.not_ok("TradeSession.accept returns ok", traceback.format_exc())

        # -- TradeSession.confirm -------------------------------------------
        try:
            resp = await trade.confirm()
            tap.test(
                "TradeSession.confirm returns confirmed",
                str(resp.state) == "confirmed",
                f"state={resp.state}",
            )
        except Exception as e:
            tap.not_ok("TradeSession.confirm returns confirmed", traceback.format_exc())

        # -- TradeSession.cancel (new trade) --------------------------------
        try:
            resp2 = await inv.startTrade(initiator=player_id, target={"id": 99})
            trade2 = resp2.session
            resp = await trade2.cancel()
            tap.test(
                "TradeSession.cancel returns cancelled",
                str(resp.state) == "cancelled",
                f"state={resp.state}",
            )
        except Exception as e:
            tap.not_ok("TradeSession.cancel returns cancelled", traceback.format_exc())


# ===========================================================================
# MatchmakingService tests
# ===========================================================================

async def test_matchmaking_service(host, port):
    mm, client = await connect(host, port, matchmaking_capnp.MatchmakingService)

    player_info = {
        "id": {"id": 42},
        "name": "TestPlayer",
        "faction": "alliance",
        "level": 10,
    }

    # -- enqueue ------------------------------------------------------------
    try:
        resp = await mm.enqueue(player=player_info, mode="arena3v3")
        tap.test(
            "MatchmakingService.enqueue returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
        tap.test(
            "MatchmakingService.enqueue returns ticket",
            resp.ticket.ticketId > 0,
            f"ticketId={resp.ticket.ticketId}",
        )
        ticket_id = resp.ticket.ticketId
    except Exception as e:
        tap.not_ok("MatchmakingService.enqueue returns ok", traceback.format_exc())
        tap.not_ok("MatchmakingService.enqueue returns ticket", "skipped")
        ticket_id = 1

    # -- getQueueStats ------------------------------------------------------
    try:
        resp = await mm.getQueueStats(mode="arena3v3")
        tap.test(
            "MatchmakingService.getQueueStats returns player count",
            resp.playersInQueue >= 1,
            f"playersInQueue={resp.playersInQueue}",
        )
    except Exception as e:
        tap.not_ok("MatchmakingService.getQueueStats returns player count", traceback.format_exc())

    # -- dequeue ------------------------------------------------------------
    try:
        resp = await mm.dequeue(ticketId=ticket_id)
        tap.test(
            "MatchmakingService.dequeue returns ok",
            str(resp.status) == "ok",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("MatchmakingService.dequeue returns ok", traceback.format_exc())

    # -- dequeue again (notFound) -------------------------------------------
    try:
        resp = await mm.dequeue(ticketId=ticket_id)
        tap.test(
            "MatchmakingService.dequeue returns notFound for removed ticket",
            str(resp.status) == "notFound",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("MatchmakingService.dequeue returns notFound for removed ticket", traceback.format_exc())

    # -- findMatch (capability passing + pipelining) ------------------------
    try:
        resp = await mm.findMatch(player=player_info, mode="duel")
        tap.test(
            "MatchmakingService.findMatch returns matchId",
            resp.matchId.id > 0,
            f"matchId={resp.matchId.id}",
        )
        controller = resp.controller
    except Exception as e:
        tap.not_ok("MatchmakingService.findMatch returns matchId", traceback.format_exc())
        controller = None

    if controller:
        # -- MatchController.getInfo ----------------------------------------
        try:
            resp = await controller.getInfo()
            tap.test(
                "MatchController.getInfo returns match info",
                str(resp.info.state) in {"waiting", "ready"} and str(resp.info.mode) == "duel",
                f"state={resp.info.state}, mode={resp.info.mode}",
            )
        except Exception as e:
            tap.not_ok("MatchController.getInfo returns match info", traceback.format_exc())

        # -- MatchController.signalReady ------------------------------------
        try:
            resp = await controller.signalReady(player={"id": 42})
            tap.test(
                "MatchController.signalReady returns ok",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
        except Exception as e:
            tap.not_ok("MatchController.signalReady returns ok", traceback.format_exc())

        # Signal ready for the second player too
        try:
            resp = await controller.signalReady(player={"id": 0})
            tap.test(
                "MatchController.signalReady allReady after both players",
                resp.allReady,
                f"allReady={resp.allReady}",
            )
        except Exception as e:
            tap.not_ok("MatchController.signalReady allReady after both players", traceback.format_exc())

        # -- MatchController.getInfo (post-ready signaling) -----------------
        try:
            resp = await controller.getInfo()
            tap.test(
                "MatchController.getInfo shows ready or inProgress after all ready",
                str(resp.info.state) in {"ready", "inProgress"},
                f"state={resp.info.state}",
            )
        except Exception as e:
            tap.not_ok("MatchController.getInfo shows ready or inProgress after all ready", traceback.format_exc())

        # -- MatchController.reportResult -----------------------------------
        try:
            resp = await controller.reportResult(result={
                "matchId": {"id": 1},
                "winningTeam": 0,
                "duration": 120,
                "playerStats": [
                    {
                        "player": player_info,
                        "kills": 5,
                        "deaths": 2,
                        "assists": 3,
                        "score": 1500,
                    },
                ],
            })
            tap.test(
                "MatchController.reportResult returns ok",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
        except Exception as e:
            tap.not_ok("MatchController.reportResult returns ok", traceback.format_exc())

        # -- MatchController.cancelMatch on new match -----------------------
        try:
            resp2 = await mm.findMatch(player=player_info, mode="battleground")
            ctrl2 = resp2.controller
            resp = await ctrl2.cancelMatch()
            tap.test(
                "MatchController.cancelMatch returns ok for ready match",
                str(resp.status) == "ok",
                f"status={resp.status}",
            )
        except Exception as e:
            tap.not_ok("MatchController.cancelMatch returns ok for ready match", traceback.format_exc())

    # -- getMatchResult (notFound) ------------------------------------------
    try:
        resp = await mm.getMatchResult(id={"id": 99999})
        tap.test(
            "MatchmakingService.getMatchResult returns notFound",
            str(resp.status) == "notFound",
            f"status={resp.status}",
        )
    except Exception as e:
        tap.not_ok("MatchmakingService.getMatchResult returns notFound", traceback.format_exc())


# ===========================================================================
# Main
# ===========================================================================

def normalize_schema_name(schema):
    if schema is None:
        return None
    if schema == "gameworld":
        return "game_world"
    return schema


async def main_async(host, port, schema):
    schema = normalize_schema_name(schema)
    print("# Cap'n Proto RPC e2e tests (Python client)")

    if schema is None:
        print("# GameWorld tests")
        await test_game_world(host, port)

        print("# ChatService tests")
        await test_chat_service(host, port + 1)

        print("# InventoryService tests")
        await test_inventory_service(host, port + 2)

        print("# MatchmakingService tests")
        await test_matchmaking_service(host, port + 3)
    elif schema == "game_world":
        print("# GameWorld tests")
        await test_game_world(host, port)
    elif schema == "chat":
        print("# ChatService tests")
        await test_chat_service(host, port)
    elif schema == "inventory":
        print("# InventoryService tests")
        await test_inventory_service(host, port)
    elif schema == "matchmaking":
        print("# MatchmakingService tests")
        await test_matchmaking_service(host, port)
    else:
        print(f"# Unknown schema: {schema}", file=sys.stderr)
        return False

    success = tap.done()
    if not success:
        print(f"# FAILED: {tap._failures} of {tap._count} tests failed", file=sys.stderr)
    return success


def main():
    parser = argparse.ArgumentParser(description="Cap'n Proto RPC e2e test client (Python)")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=4003, help="Base port")
    parser.add_argument(
        "--schema",
        default=None,
        help="Optional schema: game_world|chat|inventory|matchmaking (or gameworld)",
    )
    args = parser.parse_args()

    success = asyncio.run(capnp.run(main_async(args.host, args.port, args.schema)))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
