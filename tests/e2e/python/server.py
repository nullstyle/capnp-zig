#!/usr/bin/env python3
"""Cap'n Proto RPC e2e test server -- Python (pycapnp) implementation.

Implements all four game-dev service interfaces:
  GameWorld, ChatService, InventoryService, MatchmakingService
along with their capability interfaces:
  ChatRoom, TradeSession, MatchController

Each service listens on a consecutive port starting from --port:
  GameWorld:          port + 0
  ChatService:        port + 1
  InventoryService:   port + 2
  MatchmakingService: port + 3

Usage:
  python server.py --host 0.0.0.0 --port 4003
"""

import argparse
import asyncio
import math
import os
import sys
import time

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
# Helpers
# ---------------------------------------------------------------------------

def _now_millis():
    return int(time.time() * 1000)


def _default_player_info():
    return {"id": 0, "name": "Unknown", "faction": "neutral", "level": 1}


def _player_info_to_dict(pi):
    return {
        "id": pi.id.id,
        "name": pi.name,
        "faction": str(pi.faction),
        "level": pi.level,
    }


def _fill_player_info(builder, pi):
    builder.id.id = pi["id"]
    builder.name = pi["name"]
    builder.faction = pi["faction"]
    builder.level = pi["level"]


# ===========================================================================
# GameWorld
# ===========================================================================

class GameWorldImpl(game_world_capnp.GameWorld.Server):
    def __init__(self):
        self._next_id = 1
        self._entities = {}

    async def spawnEntity(self, request, _context, **kwargs):
        eid = self._next_id
        self._next_id += 1
        entity = {
            "id": eid,
            "kind": str(request.kind),
            "name": request.name,
            "pos": {"x": request.position.x, "y": request.position.y, "z": request.position.z},
            "health": request.maxHealth,
            "maxHealth": request.maxHealth,
            "faction": str(request.faction),
            "alive": True,
        }
        self._entities[eid] = entity
        _fill_entity(_context.results.entity, entity)
        _context.results.status = "ok"

    async def despawnEntity(self, id, _context, **kwargs):
        eid = id.id
        if eid in self._entities:
            del self._entities[eid]
            _context.results.status = "ok"
        else:
            _context.results.status = "notFound"

    async def getEntity(self, id, _context, **kwargs):
        eid = id.id
        if eid in self._entities:
            _fill_entity(_context.results.entity, self._entities[eid])
            _context.results.status = "ok"
        else:
            _context.results.status = "notFound"

    async def moveEntity(self, id, newPosition, _context, **kwargs):
        eid = id.id
        if eid not in self._entities:
            _context.results.status = "notFound"
            return
        ent = self._entities[eid]
        ent["pos"] = {"x": newPosition.x, "y": newPosition.y, "z": newPosition.z}
        _fill_entity(_context.results.entity, ent)
        _context.results.status = "ok"

    async def damageEntity(self, id, amount, _context, **kwargs):
        eid = id.id
        if eid not in self._entities:
            _context.results.status = "notFound"
            _context.results.killed = False
            return
        ent = self._entities[eid]
        ent["health"] = max(0, ent["health"] - amount)
        if ent["health"] <= 0:
            ent["alive"] = False
            _context.results.killed = True
        else:
            _context.results.killed = False
        _fill_entity(_context.results.entity, ent)
        _context.results.status = "ok"

    async def queryArea(self, query, _context, **kwargs):
        cx, cy, cz = query.center.x, query.center.y, query.center.z
        radius = query.radius
        matched = []
        for ent in self._entities.values():
            p = ent["pos"]
            dx, dy, dz = p["x"] - cx, p["y"] - cy, p["z"] - cz
            if math.sqrt(dx * dx + dy * dy + dz * dz) > radius:
                continue
            which = query.filter.which()
            if which == "byKind" and ent["kind"] != str(query.filter.byKind):
                continue
            if which == "byFaction" and ent["faction"] != str(query.filter.byFaction):
                continue
            matched.append(ent)
        elist = _context.results.init("entities", len(matched))
        for i, ent in enumerate(matched):
            _fill_entity(elist[i], ent)
        _context.results.count = len(matched)


def _fill_entity(b, ent):
    b.id.id = ent["id"]
    b.kind = ent["kind"]
    b.name = ent["name"]
    b.position.x = ent["pos"]["x"]
    b.position.y = ent["pos"]["y"]
    b.position.z = ent["pos"]["z"]
    b.health = ent["health"]
    b.maxHealth = ent["maxHealth"]
    b.faction = ent["faction"]
    b.alive = ent["alive"]


# ===========================================================================
# ChatRoom capability
# ===========================================================================

class ChatRoomImpl(chat_capnp.ChatRoom.Server):
    def __init__(self, room):
        self._room = room

    async def sendMessage(self, content, _context, **kwargs):
        msg = {
            "sender": self._room["last_player"],
            "content": content,
            "timestamp": _now_millis(),
            "kind": "normal",
        }
        self._room["messages"].append(msg)
        _fill_chat_message(_context.results.message, msg)
        _context.results.status = "ok"

    async def sendEmote(self, content, _context, **kwargs):
        msg = {
            "sender": self._room["last_player"],
            "content": content,
            "timestamp": _now_millis(),
            "kind": "emote",
        }
        self._room["messages"].append(msg)
        _fill_chat_message(_context.results.message, msg)
        _context.results.status = "ok"

    async def getHistory(self, limit, _context, **kwargs):
        msgs = self._room["messages"][-limit:] if limit > 0 else self._room["messages"]
        mlist = _context.results.init("messages", len(msgs))
        for i, m in enumerate(msgs):
            _fill_chat_message(mlist[i], m)

    async def getInfo(self, _context, **kwargs):
        _fill_room_info(_context.results.info, self._room)

    async def leave(self, _context, **kwargs):
        self._room["memberCount"] = max(0, self._room["memberCount"] - 1)
        _context.results.status = "ok"


# ===========================================================================
# ChatService
# ===========================================================================

class ChatServiceImpl(chat_capnp.ChatService.Server):
    def __init__(self):
        self._next_room_id = 1
        self._rooms = {}

    async def createRoom(self, name, topic, _context, **kwargs):
        rid = self._next_room_id
        self._next_room_id += 1
        room = {
            "id": rid,
            "name": name,
            "topic": topic,
            "memberCount": 0,
            "messages": [],
            "last_player": _default_player_info(),
        }
        self._rooms[name] = room
        _context.results.room = ChatRoomImpl(room)
        _fill_room_info(_context.results.info, room)
        _context.results.status = "ok"

    async def joinRoom(self, name, player, _context, **kwargs):
        if name not in self._rooms:
            _context.results.status = "notFound"
            return
        room = self._rooms[name]
        room["memberCount"] += 1
        room["last_player"] = _player_info_to_dict(player)
        _context.results.room = ChatRoomImpl(room)
        _context.results.status = "ok"

    async def listRooms(self, _context, **kwargs):
        rooms = list(self._rooms.values())
        rlist = _context.results.init("rooms", len(rooms))
        for i, r in enumerate(rooms):
            _fill_room_info(rlist[i], r)

    async def whisper(self, _context, **kwargs):
        params = _context.params
        from_player = getattr(params, "from")
        to_player = params.to
        content = params.content
        msg = {
            "sender": _player_info_to_dict(from_player),
            "content": content,
            "timestamp": _now_millis(),
            "kind": "whisper",
            "whisperTarget": to_player.id,
        }
        _fill_chat_message(_context.results.message, msg)
        _context.results.status = "ok"


def _fill_chat_message(b, msg):
    _fill_player_info(b.sender, msg["sender"])
    b.content = msg["content"]
    b.timestamp.unixMillis = msg["timestamp"]
    kind = msg["kind"]
    if kind == "normal":
        b.kind.normal = None
    elif kind == "emote":
        b.kind.emote = None
    elif kind == "system":
        b.kind.system = None
    elif kind == "whisper":
        b.kind.whisper.id = msg.get("whisperTarget", 0)


def _fill_room_info(b, room):
    b.id.id = room["id"]
    b.name = room["name"]
    b.memberCount = room["memberCount"]
    b.topic = room["topic"]


# ===========================================================================
# TradeSession capability
# ===========================================================================

class TradeSessionImpl(inventory_capnp.TradeSession.Server):
    def __init__(self, initiator_id, target_id):
        self._initiator = initiator_id
        self._target = target_id
        self._offered_slots = []
        self._other_offered_slots = []
        self._state = "proposing"
        self._accepted = False
        self._other_accepted = False

    async def offerItems(self, slots, _context, **kwargs):
        if self._state != "proposing":
            _context.results.status = "invalidArgument"
            return
        self._offered_slots = list(slots)
        self._accepted = False
        self._other_accepted = False
        offer = _context.results.offer
        ol = offer.init("offeredItems", len(self._offered_slots))
        for i, idx in enumerate(self._offered_slots):
            ol[i].slotIndex = idx
            ol[i].quantity = 1
        offer.accepted = False
        _context.results.status = "ok"

    async def removeItems(self, slots, _context, **kwargs):
        remove_set = set(slots)
        self._offered_slots = [s for s in self._offered_slots if s not in remove_set]
        self._accepted = False
        self._other_accepted = False
        offer = _context.results.offer
        ol = offer.init("offeredItems", len(self._offered_slots))
        for i, idx in enumerate(self._offered_slots):
            ol[i].slotIndex = idx
            ol[i].quantity = 1
        offer.accepted = False
        _context.results.status = "ok"

    async def accept(self, _context, **kwargs):
        if self._state != "proposing":
            _context.results.status = "invalidArgument"
            _context.results.state = self._state
            return
        self._accepted = True
        if self._other_accepted:
            self._state = "accepted"
        _context.results.state = self._state
        _context.results.status = "ok"

    async def confirm(self, _context, **kwargs):
        if self._accepted:
            self._state = "confirmed"
            _context.results.state = "confirmed"
            _context.results.status = "ok"
        else:
            _context.results.state = self._state
            _context.results.status = "invalidArgument"

    async def cancel(self, _context, **kwargs):
        self._state = "cancelled"
        _context.results.state = "cancelled"

    async def viewOtherOffer(self, _context, **kwargs):
        offer = _context.results.offer
        ol = offer.init("offeredItems", len(self._other_offered_slots))
        for i, idx in enumerate(self._other_offered_slots):
            ol[i].slotIndex = idx
            ol[i].quantity = 1
        offer.accepted = self._other_accepted

    async def getState(self, _context, **kwargs):
        _context.results.state = self._state


# ===========================================================================
# InventoryService
# ===========================================================================

class InventoryServiceImpl(inventory_capnp.InventoryService.Server):
    def __init__(self):
        self._inventories = {}
        self._next_slot = 0

    def _get_inv(self, pid):
        if pid not in self._inventories:
            self._inventories[pid] = []
        return self._inventories[pid]

    async def getInventory(self, player, _context, **kwargs):
        pid = player.id
        inv = self._get_inv(pid)
        _context.results.inventory.owner.id = pid
        slots = _context.results.inventory.init("slots", len(inv))
        for i, s in enumerate(inv):
            _fill_inv_slot(slots[i], s)
        _context.results.inventory.capacity = 50
        _context.results.inventory.usedSlots = len(inv)
        _context.results.status = "ok"

    async def addItem(self, player, item, quantity, _context, **kwargs):
        pid = player.id
        inv = self._get_inv(pid)
        slot_idx = self._next_slot
        self._next_slot += 1
        slot = {
            "slotIndex": slot_idx,
            "item": _item_to_dict(item),
            "quantity": quantity,
        }
        inv.append(slot)
        _fill_inv_slot(_context.results.slot, slot)
        _context.results.status = "ok"

    async def removeItem(self, player, slotIndex, quantity, _context, **kwargs):
        pid = player.id
        inv = self._get_inv(pid)
        found = False
        for i, s in enumerate(inv):
            if s["slotIndex"] == slotIndex:
                if s["quantity"] <= quantity:
                    inv.pop(i)
                else:
                    s["quantity"] -= quantity
                found = True
                break
        _context.results.status = "ok" if found else "notFound"

    async def startTrade(self, initiator, target, _context, **kwargs):
        session = TradeSessionImpl(initiator.id, target.id)
        _context.results.session = session
        _context.results.status = "ok"

    async def filterByRarity(self, player, minRarity, _context, **kwargs):
        pid = player.id
        inv = self._get_inv(pid)
        rarity_order = ["common", "uncommon", "rare", "epic", "legendary"]
        min_str = str(minRarity)
        min_idx = rarity_order.index(min_str) if min_str in rarity_order else 0
        matched = [
            s for s in inv
            if str(s["item"]["rarity"]) in rarity_order
            and rarity_order.index(str(s["item"]["rarity"])) >= min_idx
        ]
        items_list = _context.results.init("items", len(matched))
        for i, s in enumerate(matched):
            _fill_inv_slot(items_list[i], s)


def _item_to_dict(item):
    attrs = [{"name": a.name, "value": a.value} for a in item.attributes]
    return {
        "id": item.id.id,
        "name": item.name,
        "rarity": str(item.rarity),
        "level": item.level,
        "stackSize": item.stackSize,
        "attributes": attrs,
    }


def _fill_item(b, d):
    b.id.id = d["id"]
    b.name = d["name"]
    b.rarity = d["rarity"]
    b.level = d["level"]
    b.stackSize = d["stackSize"]
    attrs = b.init("attributes", len(d["attributes"]))
    for i, a in enumerate(d["attributes"]):
        attrs[i].name = a["name"]
        attrs[i].value = a["value"]


def _fill_inv_slot(b, slot):
    b.slotIndex = slot["slotIndex"]
    _fill_item(b.item, slot["item"])
    b.quantity = slot["quantity"]


# ===========================================================================
# MatchController capability
# ===========================================================================

class MatchControllerImpl(matchmaking_capnp.MatchController.Server):
    def __init__(self, match_info):
        self._match = match_info
        self._ready_players = set()

    async def getInfo(self, _context, **kwargs):
        _fill_match_info(_context.results.info, self._match)

    async def signalReady(self, player, _context, **kwargs):
        self._ready_players.add(player.id)
        total = len(self._match["teamA"]) + len(self._match["teamB"])
        all_ready = len(self._ready_players) >= total
        if all_ready:
            self._match["state"] = "inProgress"
        _context.results.allReady = all_ready
        _context.results.status = "ok"

    async def reportResult(self, result, _context, **kwargs):
        if self._match["state"] != "inProgress":
            _context.results.status = "invalidArgument"
            return
        self._match["state"] = "completed"
        _context.results.status = "ok"

    async def cancelMatch(self, _context, **kwargs):
        if self._match["state"] == "inProgress":
            _context.results.status = "invalidArgument"
            return
        self._match["state"] = "cancelled"
        _context.results.status = "ok"


# ===========================================================================
# MatchmakingService
# ===========================================================================

class MatchmakingServiceImpl(matchmaking_capnp.MatchmakingService.Server):
    def __init__(self):
        self._next_ticket = 1
        self._next_match = 1
        self._queue = {}
        self._matches = {}

    async def enqueue(self, player, mode, _context, **kwargs):
        tid = self._next_ticket
        self._next_ticket += 1
        ticket = {
            "ticketId": tid,
            "player": _player_info_to_dict(player),
            "mode": str(mode),
            "enqueuedAt": _now_millis(),
            "estimatedWaitSecs": 30,
        }
        self._queue[tid] = ticket
        _fill_queue_ticket(_context.results.ticket, ticket)
        _context.results.status = "ok"

    async def dequeue(self, ticketId, _context, **kwargs):
        if ticketId in self._queue:
            del self._queue[ticketId]
            _context.results.status = "ok"
        else:
            _context.results.status = "notFound"

    async def findMatch(self, player, mode, _context, **kwargs):
        mid = self._next_match
        self._next_match += 1
        pi = _player_info_to_dict(player)
        match_info = {
            "id": mid,
            "mode": str(mode),
            "state": "ready",
            "teamA": [pi],
            "teamB": [_default_player_info()],
            "createdAt": _now_millis(),
        }
        self._matches[mid] = match_info
        _context.results.controller = MatchControllerImpl(match_info)
        _context.results.matchId.id = mid

    async def getQueueStats(self, mode, _context, **kwargs):
        mode_str = str(mode)
        count = sum(1 for t in self._queue.values() if t["mode"] == mode_str)
        _context.results.playersInQueue = count
        _context.results.avgWaitSecs = 30

    async def getMatchResult(self, id, _context, **kwargs):
        _context.results.status = "notFound"


def _fill_queue_ticket(b, ticket):
    b.ticketId = ticket["ticketId"]
    _fill_player_info(b.player, ticket["player"])
    b.mode = ticket["mode"]
    b.enqueuedAt.unixMillis = ticket["enqueuedAt"]
    b.estimatedWaitSecs = ticket["estimatedWaitSecs"]


def _fill_match_info(b, m):
    b.id.id = m["id"]
    b.mode = m["mode"]
    b.state = m["state"]
    ta = b.init("teamA", len(m["teamA"]))
    for i, pi in enumerate(m["teamA"]):
        _fill_player_info(ta[i], pi)
    tb = b.init("teamB", len(m["teamB"]))
    for i, pi in enumerate(m["teamB"]):
        _fill_player_info(tb[i], pi)
    b.createdAt.unixMillis = m["createdAt"]


# ===========================================================================
# Server main
# ===========================================================================

async def new_connection(stream, bootstrap_factory):
    await capnp.TwoPartyServer(stream, bootstrap=bootstrap_factory()).on_disconnect()


async def run_service(host, port, factory, label):
    """Run one RPC service on the given port."""

    async def handler(stream):
        await capnp.TwoPartyServer(stream, bootstrap=factory()).on_disconnect()

    server = await capnp.AsyncIoStream.create_server(handler, host, str(port))
    print(f"[server] {label} listening on {host}:{port}", file=sys.stderr, flush=True)
    async with server:
        await server.serve_forever()


def normalize_schema_name(schema):
    if schema is None:
        return None
    if schema == "gameworld":
        return "game_world"
    return schema


def build_services_for_schema(port, schema):
    schema = normalize_schema_name(schema)
    if schema is None:
        return [
            (port, GameWorldImpl, "GameWorld"),
            (port + 1, ChatServiceImpl, "ChatService"),
            (port + 2, InventoryServiceImpl, "InventoryService"),
            (port + 3, MatchmakingServiceImpl, "MatchmakingService"),
        ]
    if schema == "game_world":
        return [(port, GameWorldImpl, "GameWorld")]
    if schema == "chat":
        return [(port, ChatServiceImpl, "ChatService")]
    if schema == "inventory":
        return [(port, InventoryServiceImpl, "InventoryService")]
    if schema == "matchmaking":
        return [(port, MatchmakingServiceImpl, "MatchmakingService")]
    raise ValueError(f"unknown schema: {schema}")


async def main_async(host, port, schema):
    services = build_services_for_schema(port, schema)

    tasks = []
    for svc_port, cls, label in services:
        tasks.append(asyncio.ensure_future(run_service(host, svc_port, cls, label)))

    # Signal readiness after a brief moment to let sockets bind
    await asyncio.sleep(0.1)
    print("READY", flush=True)

    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Cap'n Proto RPC e2e test server (Python)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=4003, help="Base port (services on port..port+3)")
    parser.add_argument(
        "--schema",
        default=None,
        help="Optional schema: game_world|chat|inventory|matchmaking (or gameworld)",
    )
    args = parser.parse_args()

    asyncio.run(capnp.run(main_async(args.host, args.port, args.schema)))


if __name__ == "__main__":
    main()
