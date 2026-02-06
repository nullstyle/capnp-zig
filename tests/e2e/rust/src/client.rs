use std::net::ToSocketAddrs;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::game_types_capnp::{Faction, Rarity, StatusCode};
use crate::game_world_capnp::EntityKind;
use crate::inventory_capnp::TradeState;
use crate::matchmaking_capnp::{GameMode, MatchState};

struct TapReporter {
    test_num: u32,
    total: u32,
    failures: u32,
}

impl TapReporter {
    fn new(total: u32) -> Self {
        println!("TAP version 14");
        println!("1..{}", total);
        Self {
            test_num: 0,
            total,
            failures: 0,
        }
    }

    fn ok(&mut self, desc: &str) {
        self.test_num += 1;
        println!("ok {} - {}", self.test_num, desc);
    }

    fn not_ok(&mut self, desc: &str, reason: &str) {
        self.test_num += 1;
        self.failures += 1;
        println!("not ok {} - {}", self.test_num, desc);
        println!("  ---");
        println!("  message: {}", reason);
        println!("  ...");
    }

    fn pass_or_fail(&mut self, desc: &str, result: Result<(), String>) {
        match result {
            Ok(()) => self.ok(desc),
            Err(e) => self.not_ok(desc, &e),
        }
    }

    fn done(self) -> bool {
        if self.test_num < self.total {
            eprintln!(
                "Warning: only ran {} of {} planned tests",
                self.test_num, self.total
            );
        }
        self.failures == 0
    }
}

macro_rules! check_eq {
    ($left:expr, $right:expr, $msg:expr) => {
        if $left != $right {
            return Err(format!("{}: expected {:?}, got {:?}", $msg, $right, $left));
        }
    };
}

macro_rules! check {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(format!("{}", $msg));
        }
    };
}

fn normalize_schema_name(schema: &str) -> &str {
    if schema == "gameworld" {
        "game_world"
    } else {
        schema
    }
}

pub async fn run(host: &str, port: u16, schema: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", host, port)
        .to_socket_addrs()?
        .next()
        .ok_or("failed to resolve address")?;

    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let stream = stream.compat();
    let (reader, writer) = stream.split();

    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );

    let mut rpc_system = RpcSystem::new(Box::new(network), None);
    let side = rpc_twoparty_capnp::Side::Server;

    match normalize_schema_name(schema) {
        "game_world" => {
            let game_world: crate::game_world_capnp::game_world::Client =
                rpc_system.bootstrap(side);
            tokio::task::spawn_local(rpc_system);
            let mut tap = TapReporter::new(7);
            tap.pass_or_fail(
                "GameWorld.spawnEntity creates entity",
                test_spawn_entity(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.getEntity retrieves entity",
                test_get_entity(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.moveEntity updates position",
                test_move_entity(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.damageEntity reduces health",
                test_damage_entity(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.damageEntity can kill",
                test_damage_kill(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.despawnEntity removes entity",
                test_despawn_entity(&game_world).await,
            );
            tap.pass_or_fail(
                "GameWorld.queryArea finds entities",
                test_query_area(&game_world).await,
            );
            if tap.done() {
                Ok(())
            } else {
                Err("Some tests failed".into())
            }
        }
        "chat" => {
            let chat_service: crate::chat_capnp::chat_service::Client = rpc_system.bootstrap(side);
            tokio::task::spawn_local(rpc_system);
            let mut tap = TapReporter::new(7);
            tap.pass_or_fail(
                "ChatService.createRoom creates room",
                test_create_room(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatRoom.sendMessage delivers",
                test_join_and_send(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatRoom.sendEmote works",
                test_send_emote(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatRoom.getHistory returns messages",
                test_get_history(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatService.listRooms lists rooms",
                test_list_rooms(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatService.whisper sends DM",
                test_whisper(&chat_service).await,
            );
            tap.pass_or_fail(
                "ChatRoom.leave reduces members",
                test_leave_room(&chat_service).await,
            );
            if tap.done() {
                Ok(())
            } else {
                Err("Some tests failed".into())
            }
        }
        "inventory" => {
            let inventory_service: crate::inventory_capnp::inventory_service::Client =
                rpc_system.bootstrap(side);
            tokio::task::spawn_local(rpc_system);
            let mut tap = TapReporter::new(6);
            tap.pass_or_fail(
                "InventoryService.addItem works",
                test_add_item(&inventory_service).await,
            );
            tap.pass_or_fail(
                "InventoryService.getInventory works",
                test_get_inventory(&inventory_service).await,
            );
            tap.pass_or_fail(
                "InventoryService.removeItem works",
                test_remove_item(&inventory_service).await,
            );
            tap.pass_or_fail(
                "InventoryService.filterByRarity works",
                test_filter_by_rarity(&inventory_service).await,
            );
            tap.pass_or_fail(
                "InventoryService.startTrade works",
                test_start_trade(&inventory_service).await,
            );
            tap.pass_or_fail(
                "TradeSession full flow",
                test_trade_flow(&inventory_service).await,
            );
            if tap.done() {
                Ok(())
            } else {
                Err("Some tests failed".into())
            }
        }
        "matchmaking" => {
            let matchmaking_service: crate::matchmaking_capnp::matchmaking_service::Client =
                rpc_system.bootstrap(side);
            tokio::task::spawn_local(rpc_system);
            let mut tap = TapReporter::new(5);
            tap.pass_or_fail(
                "MatchmakingService.enqueue works",
                test_enqueue(&matchmaking_service).await,
            );
            tap.pass_or_fail(
                "MatchmakingService.dequeue works",
                test_dequeue(&matchmaking_service).await,
            );
            tap.pass_or_fail(
                "MatchmakingService.findMatch works",
                test_find_match(&matchmaking_service).await,
            );
            tap.pass_or_fail(
                "MatchController signalReady+getInfo",
                test_match_controller(&matchmaking_service).await,
            );
            tap.pass_or_fail(
                "MatchmakingService.getQueueStats works",
                test_queue_stats(&matchmaking_service).await,
            );
            if tap.done() {
                Ok(())
            } else {
                Err("Some tests failed".into())
            }
        }
        _ => {
            eprintln!("unknown schema: {}", schema);
            Err("Unknown schema".into())
        }
    }
}

// -- GameWorld tests --

async fn spawn_test_entity(
    gw: &crate::game_world_capnp::game_world::Client,
) -> Result<u64, String> {
    let mut req = gw.spawn_entity_request();
    let mut sr = req.get().init_request();
    sr.set_kind(EntityKind::Player);
    sr.reborrow().set_name("TestHero");
    let mut pos = sr.reborrow().init_position();
    pos.set_x(10.0);
    pos.set_y(20.0);
    pos.set_z(30.0);
    sr.set_faction(Faction::Alliance);
    sr.set_max_health(100);

    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "spawn status"
    );
    Ok(r.get_entity()
        .map_err(|e| e.to_string())?
        .get_id()
        .map_err(|e| e.to_string())?
        .get_id())
}

async fn test_spawn_entity(gw: &crate::game_world_capnp::game_world::Client) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    check!(id > 0, "entity id should be positive");
    Ok(())
}

async fn test_get_entity(gw: &crate::game_world_capnp::game_world::Client) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    let mut req = gw.get_entity_request();
    req.get().init_id().set_id(id);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "get status"
    );
    let ent = r.get_entity().map_err(|e| e.to_string())?;
    check_eq!(
        ent.get_name()
            .map_err(|e| e.to_string())?
            .to_str()
            .map_err(|e| e.to_string())?,
        "TestHero",
        "name"
    );
    check_eq!(ent.get_health(), 100, "health");
    check_eq!(ent.get_alive(), true, "alive");
    Ok(())
}

async fn test_move_entity(gw: &crate::game_world_capnp::game_world::Client) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    let mut req = gw.move_entity_request();
    req.get().init_id().set_id(id);
    let mut pos = req.get().init_new_position();
    pos.set_x(99.0);
    pos.set_y(88.0);
    pos.set_z(77.0);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "move status"
    );
    let p = r
        .get_entity()
        .map_err(|e| e.to_string())?
        .get_position()
        .map_err(|e| e.to_string())?;
    check_eq!(p.get_x(), 99.0, "x");
    check_eq!(p.get_y(), 88.0, "y");
    Ok(())
}

async fn test_damage_entity(
    gw: &crate::game_world_capnp::game_world::Client,
) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    let mut req = gw.damage_entity_request();
    req.get().init_id().set_id(id);
    req.get().set_amount(30);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "damage status"
    );
    check_eq!(
        r.get_entity().map_err(|e| e.to_string())?.get_health(),
        70,
        "health after damage"
    );
    check_eq!(r.get_killed(), false, "not killed");
    Ok(())
}

async fn test_damage_kill(gw: &crate::game_world_capnp::game_world::Client) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    let mut req = gw.damage_entity_request();
    req.get().init_id().set_id(id);
    req.get().set_amount(150);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(r.get_killed(), true, "killed");
    let ent = r.get_entity().map_err(|e| e.to_string())?;
    check_eq!(ent.get_alive(), false, "dead");
    check_eq!(ent.get_health(), 0, "health 0");
    Ok(())
}

async fn test_despawn_entity(
    gw: &crate::game_world_capnp::game_world::Client,
) -> Result<(), String> {
    let id = spawn_test_entity(gw).await?;
    let mut req = gw.despawn_entity_request();
    req.get().init_id().set_id(id);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "despawn"
    );
    let mut req2 = gw.get_entity_request();
    req2.get().init_id().set_id(id);
    let resp2 = req2.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp2
            .get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::NotFound,
        "gone"
    );
    Ok(())
}

async fn test_query_area(gw: &crate::game_world_capnp::game_world::Client) -> Result<(), String> {
    let mut req = gw.spawn_entity_request();
    let mut sr = req.get().init_request();
    sr.set_kind(EntityKind::Monster);
    sr.reborrow().set_name("NearMonster");
    let mut p = sr.reborrow().init_position();
    p.set_x(0.0);
    p.set_y(0.0);
    p.set_z(0.0);
    sr.set_faction(Faction::Horde);
    sr.set_max_health(50);
    req.send().promise.await.map_err(|e| e.to_string())?;

    let mut qr = gw.query_area_request();
    let mut q = qr.get().init_query();
    let mut c = q.reborrow().init_center();
    c.set_x(0.0);
    c.set_y(0.0);
    c.set_z(0.0);
    q.reborrow().set_radius(1000.0);
    q.init_filter().set_all(());
    let resp = qr.send().promise.await.map_err(|e| e.to_string())?;
    let count = resp.get().map_err(|e| e.to_string())?.get_count();
    check!(
        count >= 1,
        format!("queryArea should find >= 1, got {}", count)
    );
    Ok(())
}

// -- Chat tests --

async fn test_create_room(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut req = cs.create_room_request();
    req.get().set_name("general");
    req.get().set_topic("General chat");
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "create room"
    );
    let name = r
        .get_info()
        .map_err(|e| e.to_string())?
        .get_name()
        .map_err(|e| e.to_string())?
        .to_str()
        .map_err(|e| e.to_string())?;
    check_eq!(name, "general", "room name");
    Ok(())
}

async fn test_join_and_send(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut cr = cs.create_room_request();
    cr.get().set_name("test-chat");
    cr.get().set_topic("Test");
    cr.send().promise.await.map_err(|e| e.to_string())?;

    let mut jr = cs.join_room_request();
    jr.get().set_name("test-chat");
    let mut pi = jr.get().init_player();
    pi.reborrow().init_id().set_id(42);
    pi.reborrow().set_name("Alice");
    pi.reborrow().set_faction(Faction::Alliance);
    pi.set_level(10);
    let resp = jr.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "join"
    );
    let room = r.get_room().map_err(|e| e.to_string())?;

    let mut sm = room.send_message_request();
    sm.get().set_content("Hello, world!");
    let resp = sm.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "send"
    );
    let content = r
        .get_message()
        .map_err(|e| e.to_string())?
        .get_content()
        .map_err(|e| e.to_string())?
        .to_str()
        .map_err(|e| e.to_string())?;
    check_eq!(content, "Hello, world!", "content");
    Ok(())
}

async fn test_send_emote(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut cr = cs.create_room_request();
    cr.get().set_name("emote-room");
    cr.get().set_topic("Emotes");
    cr.send().promise.await.map_err(|e| e.to_string())?;
    let mut jr = cs.join_room_request();
    jr.get().set_name("emote-room");
    let mut pi = jr.get().init_player();
    pi.reborrow().init_id().set_id(43);
    pi.reborrow().set_name("Bob");
    pi.reborrow().set_faction(Faction::Horde);
    pi.set_level(5);
    let resp = jr.send().promise.await.map_err(|e| e.to_string())?;
    let room = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_room()
        .map_err(|e| e.to_string())?;
    let mut er = room.send_emote_request();
    er.get().set_content("dances");
    let resp = er.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "emote"
    );
    Ok(())
}

async fn test_get_history(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut cr = cs.create_room_request();
    cr.get().set_name("history-room");
    cr.get().set_topic("History");
    cr.send().promise.await.map_err(|e| e.to_string())?;
    let mut jr = cs.join_room_request();
    jr.get().set_name("history-room");
    let mut pi = jr.get().init_player();
    pi.reborrow().init_id().set_id(44);
    pi.reborrow().set_name("Carol");
    pi.reborrow().set_faction(Faction::Pirates);
    pi.set_level(20);
    let resp = jr.send().promise.await.map_err(|e| e.to_string())?;
    let room = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_room()
        .map_err(|e| e.to_string())?;
    let mut s1 = room.send_message_request();
    s1.get().set_content("First");
    s1.send().promise.await.map_err(|e| e.to_string())?;
    let mut s2 = room.send_message_request();
    s2.get().set_content("Second");
    s2.send().promise.await.map_err(|e| e.to_string())?;
    let mut hr = room.get_history_request();
    hr.get().set_limit(10);
    let resp = hr.send().promise.await.map_err(|e| e.to_string())?;
    let msgs = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_messages()
        .map_err(|e| e.to_string())?;
    check!(
        msgs.len() >= 2,
        format!("expected >= 2 messages, got {}", msgs.len())
    );
    Ok(())
}

async fn test_list_rooms(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut req = cs.list_rooms_request();
    let _ = req.get();
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let rooms = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_rooms()
        .map_err(|e| e.to_string())?;
    check!(
        rooms.len() >= 1,
        format!("expected >= 1 room, got {}", rooms.len())
    );
    Ok(())
}

async fn test_whisper(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut req = cs.whisper_request();
    let mut from = req.get().init_from();
    from.reborrow().init_id().set_id(42);
    from.reborrow().set_name("Alice");
    from.reborrow().set_faction(Faction::Alliance);
    from.set_level(10);
    req.get().init_to().set_id(43);
    req.get().set_content("Hey Bob!");
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "whisper"
    );
    let c = r
        .get_message()
        .map_err(|e| e.to_string())?
        .get_content()
        .map_err(|e| e.to_string())?
        .to_str()
        .map_err(|e| e.to_string())?;
    check_eq!(c, "Hey Bob!", "whisper content");
    Ok(())
}

async fn test_leave_room(cs: &crate::chat_capnp::chat_service::Client) -> Result<(), String> {
    let mut cr = cs.create_room_request();
    cr.get().set_name("leave-room");
    cr.get().set_topic("Leave");
    cr.send().promise.await.map_err(|e| e.to_string())?;
    let mut jr = cs.join_room_request();
    jr.get().set_name("leave-room");
    let mut pi = jr.get().init_player();
    pi.reborrow().init_id().set_id(45);
    pi.reborrow().set_name("Dave");
    pi.reborrow().set_faction(Faction::Neutral);
    pi.set_level(1);
    let resp = jr.send().promise.await.map_err(|e| e.to_string())?;
    let room = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_room()
        .map_err(|e| e.to_string())?;
    let mut leave = room.leave_request();
    let _ = leave.get();
    let resp = leave.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "leave"
    );
    Ok(())
}

// -- Inventory tests --

async fn add_test_item(
    inv: &crate::inventory_capnp::inventory_service::Client,
    player_id: u64,
    item_id: u64,
    name: &str,
    r: Rarity,
    level: u16,
    qty: u32,
) -> Result<u16, String> {
    let mut req = inv.add_item_request();
    req.get().init_player().set_id(player_id);
    let mut item = req.get().init_item();
    item.reborrow().init_id().set_id(item_id);
    item.reborrow().set_name(name);
    item.reborrow().set_rarity(r);
    item.reborrow().set_level(level);
    item.set_stack_size(qty);
    req.get().set_quantity(qty);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let res = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        res.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "add item"
    );
    Ok(res.get_slot().map_err(|e| e.to_string())?.get_slot_index())
}

async fn test_add_item(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let idx = add_test_item(inv, 100, 1, "Iron Sword", Rarity::Common, 1, 1).await?;
    check_eq!(idx, 0, "first slot index");
    Ok(())
}

async fn test_get_inventory(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let mut req = inv.get_inventory_request();
    req.get().init_player().set_id(100);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "get inv"
    );
    let inv_view = r.get_inventory().map_err(|e| e.to_string())?;
    check!(
        inv_view.get_slots().map_err(|e| e.to_string())?.len() >= 1,
        "should have >= 1 slot"
    );
    check!(
        inv_view.get_capacity() >= inv_view.get_used_slots(),
        "capacity >= used slots"
    );
    Ok(())
}

async fn test_remove_item(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let idx = add_test_item(inv, 200, 2, "Potion", Rarity::Common, 1, 5).await?;
    let mut req = inv.remove_item_request();
    req.get().init_player().set_id(200);
    req.get().set_slot_index(idx);
    req.get().set_quantity(3);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "remove"
    );
    Ok(())
}

async fn test_filter_by_rarity(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let pid = 300;
    add_test_item(inv, pid, 10, "Wooden Shield", Rarity::Common, 1, 1).await?;
    add_test_item(inv, pid, 11, "Steel Armor", Rarity::Rare, 10, 1).await?;
    add_test_item(inv, pid, 12, "Dragon Blade", Rarity::Legendary, 50, 1).await?;
    let mut req = inv.filter_by_rarity_request();
    req.get().init_player().set_id(pid);
    req.get().set_min_rarity(Rarity::Rare);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let items = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_items()
        .map_err(|e| e.to_string())?;
    check_eq!(items.len(), 2, "should have 2 rare+ items");
    Ok(())
}

async fn test_start_trade(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let mut req = inv.start_trade_request();
    req.get().init_initiator().set_id(100);
    req.get().init_target().set_id(200);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "start trade"
    );
    let _session = r.get_session().map_err(|e| e.to_string())?;
    Ok(())
}

async fn test_trade_flow(
    inv: &crate::inventory_capnp::inventory_service::Client,
) -> Result<(), String> {
    let mut req = inv.start_trade_request();
    req.get().init_initiator().set_id(100);
    req.get().init_target().set_id(200);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let session = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_session()
        .map_err(|e| e.to_string())?;

    let mut or = session.offer_items_request();
    {
        let mut slots = or.get().init_slots(2);
        slots.set(0, 0);
        slots.set(1, 1);
    }
    let resp = or.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "offer"
    );

    let mut state_req = session.get_state_request();
    let _ = state_req.get();
    let resp = state_req.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_state()
            .map_err(|e| e.to_string())?,
        TradeState::Proposing,
        "proposing"
    );

    let mut confirm_req = session.confirm_request();
    let _ = confirm_req.get();
    let resp = confirm_req
        .send()
        .promise
        .await
        .map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_state()
            .map_err(|e| e.to_string())?,
        TradeState::Confirmed,
        "confirmed"
    );
    Ok(())
}

// -- Matchmaking tests --

fn set_test_player(builder: &mut crate::game_types_capnp::player_info::Builder<'_>, id: u64) {
    builder.reborrow().init_id().set_id(id);
    builder.reborrow().set_name(&format!("Player_{}", id));
    builder.reborrow().set_faction(Faction::Alliance);
    builder.set_level(30);
}

async fn test_enqueue(
    mm: &crate::matchmaking_capnp::matchmaking_service::Client,
) -> Result<(), String> {
    let mut req = mm.enqueue_request();
    set_test_player(&mut req.get().init_player(), 500);
    req.get().set_mode(GameMode::Duel);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "enqueue"
    );
    check!(
        r.get_ticket().map_err(|e| e.to_string())?.get_ticket_id() > 0,
        "ticket id > 0"
    );
    Ok(())
}

async fn test_dequeue(
    mm: &crate::matchmaking_capnp::matchmaking_service::Client,
) -> Result<(), String> {
    let mut er = mm.enqueue_request();
    set_test_player(&mut er.get().init_player(), 501);
    er.get().set_mode(GameMode::Arena3v3);
    let resp = er.send().promise.await.map_err(|e| e.to_string())?;
    let tid = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_ticket()
        .map_err(|e| e.to_string())?
        .get_ticket_id();
    let mut dr = mm.dequeue_request();
    dr.get().set_ticket_id(tid);
    let resp = dr.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp.get()
            .map_err(|e| e.to_string())?
            .get_status()
            .map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "dequeue"
    );
    Ok(())
}

async fn test_find_match(
    mm: &crate::matchmaking_capnp::matchmaking_service::Client,
) -> Result<(), String> {
    let mut req = mm.find_match_request();
    set_test_player(&mut req.get().init_player(), 502);
    req.get().set_mode(GameMode::Duel);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check!(
        r.get_match_id().map_err(|e| e.to_string())?.get_id() > 0,
        "match id > 0"
    );
    let _ctrl = r.get_controller().map_err(|e| e.to_string())?;
    Ok(())
}

async fn test_match_controller(
    mm: &crate::matchmaking_capnp::matchmaking_service::Client,
) -> Result<(), String> {
    let mut req = mm.find_match_request();
    set_test_player(&mut req.get().init_player(), 503);
    req.get().set_mode(GameMode::Duel);
    let resp = req.send().promise.await.map_err(|e| e.to_string())?;
    let ctrl = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_controller()
        .map_err(|e| e.to_string())?;

    let mut info_req = ctrl.get_info_request();
    let _ = info_req.get();
    let resp = info_req.send().promise.await.map_err(|e| e.to_string())?;
    let state = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_info()
        .map_err(|e| e.to_string())?
        .get_state()
        .map_err(|e| e.to_string())?;
    check!(
        state == MatchState::Waiting || state == MatchState::Ready,
        format!("waiting or ready: got {:?}", state)
    );

    let mut rr = ctrl.signal_ready_request();
    rr.get().init_player().set_id(503);
    let resp = rr.send().promise.await.map_err(|e| e.to_string())?;
    let r = resp.get().map_err(|e| e.to_string())?;
    check_eq!(
        r.get_status().map_err(|e| e.to_string())?,
        StatusCode::Ok,
        "signal ready"
    );

    let mut rr2 = ctrl.signal_ready_request();
    rr2.get().init_player().set_id(1503);
    let resp2 = rr2.send().promise.await.map_err(|e| e.to_string())?;
    check_eq!(
        resp2.get().map_err(|e| e.to_string())?.get_all_ready(),
        true,
        "all ready"
    );
    Ok(())
}

async fn test_queue_stats(
    mm: &crate::matchmaking_capnp::matchmaking_service::Client,
) -> Result<(), String> {
    let mut r1 = mm.enqueue_request();
    set_test_player(&mut r1.get().init_player(), 600);
    r1.get().set_mode(GameMode::Battleground);
    r1.send().promise.await.map_err(|e| e.to_string())?;
    let mut r2 = mm.enqueue_request();
    set_test_player(&mut r2.get().init_player(), 601);
    r2.get().set_mode(GameMode::Battleground);
    r2.send().promise.await.map_err(|e| e.to_string())?;

    let mut sr = mm.get_queue_stats_request();
    sr.get().set_mode(GameMode::Battleground);
    let resp = sr.send().promise.await.map_err(|e| e.to_string())?;
    let count = resp
        .get()
        .map_err(|e| e.to_string())?
        .get_players_in_queue();
    check!(count >= 2, format!("expected >= 2 in queue, got {}", count));
    Ok(())
}
