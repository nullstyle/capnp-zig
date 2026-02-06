use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};

use capnp::capability::Promise;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::chat_capnp::{chat_room, chat_service};
use crate::game_types_capnp::{Faction, Rarity, StatusCode};
use crate::game_world_capnp::{area_query, game_world, EntityKind};
use crate::inventory_capnp::{inventory_service, trade_session, TradeState};
use crate::matchmaking_capnp::{match_controller, matchmaking_service, GameMode, MatchState};

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// Shared data structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PlayerInfoData {
    id: u64,
    name: String,
    faction: Faction,
    level: u16,
}

fn read_player_info(
    reader: crate::game_types_capnp::player_info::Reader<'_>,
) -> Result<PlayerInfoData, capnp::Error> {
    Ok(PlayerInfoData {
        id: reader.get_id()?.get_id(),
        name: reader.get_name()?.to_string()?,
        faction: reader.get_faction()?,
        level: reader.get_level(),
    })
}

fn build_player_info(
    builder: &mut crate::game_types_capnp::player_info::Builder<'_>,
    p: &PlayerInfoData,
) {
    builder.reborrow().init_id().set_id(p.id);
    builder.reborrow().set_name(&p.name);
    builder.reborrow().set_faction(p.faction);
    builder.set_level(p.level);
}

// ---------------------------------------------------------------------------
// GameWorld implementation
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct EntityData {
    id: u64,
    kind: EntityKind,
    name: String,
    position: [f32; 3],
    health: i32,
    max_health: i32,
    faction: Faction,
    alive: bool,
}

fn set_entity(builder: &mut crate::game_world_capnp::entity::Builder<'_>, e: &EntityData) {
    builder.reborrow().init_id().set_id(e.id);
    builder.reborrow().set_kind(e.kind);
    builder.reborrow().set_name(&e.name);
    let mut pos = builder.reborrow().init_position();
    pos.set_x(e.position[0]);
    pos.set_y(e.position[1]);
    pos.set_z(e.position[2]);
    builder.reborrow().set_health(e.health);
    builder.reborrow().set_max_health(e.max_health);
    builder.reborrow().set_faction(e.faction);
    builder.set_alive(e.alive);
}

struct GameWorldImpl {
    state: Arc<Mutex<GameWorldState>>,
}

struct GameWorldState {
    next_id: u64,
    entities: HashMap<u64, EntityData>,
}

impl GameWorldImpl {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(GameWorldState {
                next_id: 1,
                entities: HashMap::new(),
            })),
        }
    }
}

impl game_world::Server for GameWorldImpl {
    fn spawn_entity(
        &mut self,
        params: game_world::SpawnEntityParams,
        mut results: game_world::SpawnEntityResults,
    ) -> Promise<(), capnp::Error> {
        let req = pry!(pry!(params.get()).get_request());
        let kind = pry!(req.get_kind());
        let name = pry!(req.get_name()).to_string().unwrap_or_default();
        let p = pry!(req.get_position());
        let position = [p.get_x(), p.get_y(), p.get_z()];
        let fac = pry!(req.get_faction());
        let max_health = req.get_max_health();

        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        let entity = EntityData {
            id,
            kind,
            name,
            position,
            health: max_health,
            max_health,
            faction: fac,
            alive: true,
        };
        st.entities.insert(id, entity.clone());

        let mut r = results.get();
        set_entity(&mut r.reborrow().init_entity(), &entity);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn despawn_entity(
        &mut self,
        params: game_world::DespawnEntityParams,
        mut results: game_world::DespawnEntityResults,
    ) -> Promise<(), capnp::Error> {
        let id = pry!(pry!(params.get()).get_id()).get_id();
        let mut st = self.state.lock().unwrap();
        if st.entities.remove(&id).is_some() {
            results.get().set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn get_entity(
        &mut self,
        params: game_world::GetEntityParams,
        mut results: game_world::GetEntityResults,
    ) -> Promise<(), capnp::Error> {
        let id = pry!(pry!(params.get()).get_id()).get_id();
        let st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(e) = st.entities.get(&id) {
            set_entity(&mut r.reborrow().init_entity(), e);
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn move_entity(
        &mut self,
        params: game_world::MoveEntityParams,
        mut results: game_world::MoveEntityResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let id = pry!(p.get_id()).get_id();
        let np = pry!(p.get_new_position());
        let pos = [np.get_x(), np.get_y(), np.get_z()];

        let mut st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(e) = st.entities.get_mut(&id) {
            e.position = pos;
            let e = e.clone();
            set_entity(&mut r.reborrow().init_entity(), &e);
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn damage_entity(
        &mut self,
        params: game_world::DamageEntityParams,
        mut results: game_world::DamageEntityResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let id = pry!(p.get_id()).get_id();
        let amount = p.get_amount();

        let mut st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(e) = st.entities.get_mut(&id) {
            e.health -= amount;
            let killed = e.health <= 0;
            if killed {
                e.alive = false;
                e.health = 0;
            }
            let e = e.clone();
            set_entity(&mut r.reborrow().init_entity(), &e);
            r.set_killed(killed);
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn query_area(
        &mut self,
        params: game_world::QueryAreaParams,
        mut results: game_world::QueryAreaResults,
    ) -> Promise<(), capnp::Error> {
        let q = pry!(pry!(params.get()).get_query());
        let c = pry!(q.get_center());
        let (cx, cy, cz) = (c.get_x(), c.get_y(), c.get_z());
        let radius = q.get_radius();
        let filter = pry!(q.get_filter().which());

        // Pre-extract filter values before the loop
        enum Filter {
            All,
            ByKind(EntityKind),
            ByFaction(Faction),
        }
        let filter = match filter {
            area_query::filter::Which::All(()) => Filter::All,
            area_query::filter::Which::ByKind(k) => Filter::ByKind(pry!(k)),
            area_query::filter::Which::ByFaction(f) => Filter::ByFaction(pry!(f)),
        };

        let st = self.state.lock().unwrap();
        let mut matched: Vec<EntityData> = Vec::new();

        for e in st.entities.values() {
            let dx = e.position[0] - cx;
            let dy = e.position[1] - cy;
            let dz = e.position[2] - cz;
            if (dx * dx + dy * dy + dz * dz).sqrt() > radius {
                continue;
            }
            match &filter {
                Filter::All => {}
                Filter::ByKind(k) => {
                    if *k != e.kind {
                        continue;
                    }
                }
                Filter::ByFaction(f) => {
                    if *f != e.faction {
                        continue;
                    }
                }
            }
            matched.push(e.clone());
        }

        let count = matched.len() as u32;
        let mut r = results.get();
        let mut list = r.reborrow().init_entities(count);
        for (i, e) in matched.iter().enumerate() {
            set_entity(&mut list.reborrow().get(i as u32), e);
        }
        r.set_count(count);
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// Chat implementation
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ChatMessageData {
    sender: PlayerInfoData,
    content: String,
    timestamp_millis: i64,
    is_emote: bool,
    whisper_target: Option<u64>,
}

fn build_chat_message(
    builder: &mut crate::chat_capnp::chat_message::Builder<'_>,
    msg: &ChatMessageData,
) {
    build_player_info(&mut builder.reborrow().init_sender(), &msg.sender);
    builder.reborrow().set_content(&msg.content);
    builder
        .reborrow()
        .init_timestamp()
        .set_unix_millis(msg.timestamp_millis);
    if let Some(target) = msg.whisper_target {
        builder.reborrow().init_kind().init_whisper().set_id(target);
    } else if msg.is_emote {
        builder.reborrow().init_kind().set_emote(());
    } else {
        builder.reborrow().init_kind().set_normal(());
    }
}

#[derive(Clone)]
struct ChatRoomData {
    id: u64,
    name: String,
    topic: String,
    messages: Vec<ChatMessageData>,
    member_count: u32,
}

struct ChatState {
    rooms: HashMap<String, ChatRoomData>,
    next_room_id: u64,
}

struct ChatRoomImpl {
    room_name: String,
    player: PlayerInfoData,
    state: Arc<Mutex<ChatState>>,
}

impl chat_room::Server for ChatRoomImpl {
    fn send_message(
        &mut self,
        params: chat_room::SendMessageParams,
        mut results: chat_room::SendMessageResults,
    ) -> Promise<(), capnp::Error> {
        let content = pry!(pry!(params.get()).get_content())
            .to_string()
            .unwrap_or_default();
        let msg = ChatMessageData {
            sender: self.player.clone(),
            content,
            timestamp_millis: now_millis(),
            is_emote: false,
            whisper_target: None,
        };
        let mut st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get_mut(&self.room_name) {
            room.messages.push(msg.clone());
            let mut r = results.get();
            build_chat_message(&mut r.reborrow().init_message(), &msg);
            r.set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn send_emote(
        &mut self,
        params: chat_room::SendEmoteParams,
        mut results: chat_room::SendEmoteResults,
    ) -> Promise<(), capnp::Error> {
        let content = pry!(pry!(params.get()).get_content())
            .to_string()
            .unwrap_or_default();
        let msg = ChatMessageData {
            sender: self.player.clone(),
            content,
            timestamp_millis: now_millis(),
            is_emote: true,
            whisper_target: None,
        };
        let mut st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get_mut(&self.room_name) {
            room.messages.push(msg.clone());
            let mut r = results.get();
            build_chat_message(&mut r.reborrow().init_message(), &msg);
            r.set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn get_history(
        &mut self,
        params: chat_room::GetHistoryParams,
        mut results: chat_room::GetHistoryResults,
    ) -> Promise<(), capnp::Error> {
        let limit = pry!(params.get()).get_limit() as usize;
        let st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get(&self.room_name) {
            let msgs: Vec<_> = room
                .messages
                .iter()
                .rev()
                .take(limit)
                .rev()
                .cloned()
                .collect();
            let mut list = results.get().init_messages(msgs.len() as u32);
            for (i, msg) in msgs.iter().enumerate() {
                build_chat_message(&mut list.reborrow().get(i as u32), msg);
            }
        }
        Promise::ok(())
    }

    fn get_info(
        &mut self,
        _params: chat_room::GetInfoParams,
        mut results: chat_room::GetInfoResults,
    ) -> Promise<(), capnp::Error> {
        let st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get(&self.room_name) {
            let mut info = results.get().init_info();
            info.reborrow().init_id().set_id(room.id);
            info.reborrow().set_name(&room.name);
            info.reborrow().set_member_count(room.member_count);
            info.set_topic(&room.topic);
        }
        Promise::ok(())
    }

    fn leave(
        &mut self,
        _params: chat_room::LeaveParams,
        mut results: chat_room::LeaveResults,
    ) -> Promise<(), capnp::Error> {
        let mut st = self.state.lock().unwrap();
        if let Some(room) = st.rooms.get_mut(&self.room_name) {
            room.member_count = room.member_count.saturating_sub(1);
            results.get().set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }
}

struct ChatServiceImpl {
    state: Arc<Mutex<ChatState>>,
}

impl ChatServiceImpl {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ChatState {
                rooms: HashMap::new(),
                next_room_id: 1,
            })),
        }
    }
}

impl chat_service::Server for ChatServiceImpl {
    fn create_room(
        &mut self,
        params: chat_service::CreateRoomParams,
        mut results: chat_service::CreateRoomResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let name = pry!(p.get_name()).to_string().unwrap_or_default();
        let topic = pry!(p.get_topic()).to_string().unwrap_or_default();

        let mut st = self.state.lock().unwrap();
        if st.rooms.contains_key(&name) {
            results.get().set_status(StatusCode::AlreadyExists);
            return Promise::ok(());
        }
        let id = st.next_room_id;
        st.next_room_id += 1;
        st.rooms.insert(
            name.clone(),
            ChatRoomData {
                id,
                name: name.clone(),
                topic: topic.clone(),
                messages: Vec::new(),
                member_count: 0,
            },
        );

        let room_impl = ChatRoomImpl {
            room_name: name.clone(),
            player: PlayerInfoData {
                id: 0,
                name: "system".into(),
                faction: Faction::Neutral,
                level: 0,
            },
            state: self.state.clone(),
        };
        let room_client: chat_room::Client = capnp_rpc::new_client(room_impl);

        let mut r = results.get();
        r.reborrow().set_room(room_client);
        let mut info = r.reborrow().init_info();
        info.reborrow().init_id().set_id(id);
        info.reborrow().set_name(&name);
        info.reborrow().set_member_count(0);
        info.set_topic(&topic);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn join_room(
        &mut self,
        params: chat_service::JoinRoomParams,
        mut results: chat_service::JoinRoomResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let name = pry!(p.get_name()).to_string().unwrap_or_default();
        let player = pry!(read_player_info(pry!(p.get_player())));

        let mut st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(room) = st.rooms.get_mut(&name) {
            room.member_count += 1;
            let room_impl = ChatRoomImpl {
                room_name: name.clone(),
                player,
                state: self.state.clone(),
            };
            r.set_room(capnp_rpc::new_client(room_impl));
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn list_rooms(
        &mut self,
        _params: chat_service::ListRoomsParams,
        mut results: chat_service::ListRoomsResults,
    ) -> Promise<(), capnp::Error> {
        let st = self.state.lock().unwrap();
        let rooms: Vec<_> = st.rooms.values().collect();
        let mut list = results.get().init_rooms(rooms.len() as u32);
        for (i, room) in rooms.iter().enumerate() {
            let mut info = list.reborrow().get(i as u32);
            info.reborrow().init_id().set_id(room.id);
            info.reborrow().set_name(&room.name);
            info.reborrow().set_member_count(room.member_count);
            info.set_topic(&room.topic);
        }
        Promise::ok(())
    }

    fn whisper(
        &mut self,
        params: chat_service::WhisperParams,
        mut results: chat_service::WhisperResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let from = pry!(read_player_info(pry!(p.get_from())));
        let to_id = pry!(p.get_to()).get_id();
        let content = pry!(p.get_content()).to_string().unwrap_or_default();

        let msg = ChatMessageData {
            sender: from,
            content,
            timestamp_millis: now_millis(),
            is_emote: false,
            whisper_target: Some(to_id),
        };
        let mut r = results.get();
        build_chat_message(&mut r.reborrow().init_message(), &msg);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// Inventory implementation
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct InventorySlotData {
    slot_index: u16,
    item_id: u64,
    item_name: String,
    item_rarity: Rarity,
    item_level: u16,
    quantity: u32,
}

fn build_inventory_slot(
    builder: &mut crate::inventory_capnp::inventory_slot::Builder<'_>,
    s: &InventorySlotData,
) {
    builder.reborrow().set_slot_index(s.slot_index);
    let mut item = builder.reborrow().init_item();
    item.reborrow().init_id().set_id(s.item_id);
    item.reborrow().set_name(&s.item_name);
    item.reborrow().set_rarity(s.item_rarity);
    item.reborrow().set_level(s.item_level);
    item.set_stack_size(s.quantity);
    builder.set_quantity(s.quantity);
}

fn rarity_rank(r: Rarity) -> u8 {
    match r {
        Rarity::Common => 0,
        Rarity::Uncommon => 1,
        Rarity::Rare => 2,
        Rarity::Epic => 3,
        Rarity::Legendary => 4,
    }
}

struct InventoryState {
    inventories: HashMap<u64, Vec<InventorySlotData>>,
}

struct InventoryServiceImpl {
    state: Arc<Mutex<InventoryState>>,
}

impl InventoryServiceImpl {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InventoryState {
                inventories: HashMap::new(),
            })),
        }
    }
}

impl inventory_service::Server for InventoryServiceImpl {
    fn get_inventory(
        &mut self,
        params: inventory_service::GetInventoryParams,
        mut results: inventory_service::GetInventoryResults,
    ) -> Promise<(), capnp::Error> {
        let player_id = pry!(pry!(params.get()).get_player()).get_id();
        let st = self.state.lock().unwrap();
        let mut r = results.get();
        let mut inv = r.reborrow().init_inventory();
        inv.reborrow().init_owner().set_id(player_id);
        if let Some(slots) = st.inventories.get(&player_id) {
            let n = slots.len() as u32;
            let mut sl = inv.reborrow().init_slots(n);
            for (i, s) in slots.iter().enumerate() {
                build_inventory_slot(&mut sl.reborrow().get(i as u32), s);
            }
            inv.reborrow().set_used_slots(n as u16);
        } else {
            inv.reborrow().init_slots(0);
            inv.reborrow().set_used_slots(0);
        }
        inv.set_capacity(50);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn add_item(
        &mut self,
        params: inventory_service::AddItemParams,
        mut results: inventory_service::AddItemResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let player_id = pry!(p.get_player()).get_id();
        let item = pry!(p.get_item());
        let quantity = p.get_quantity();

        let mut slot_data = InventorySlotData {
            slot_index: 0,
            item_id: pry!(item.get_id()).get_id(),
            item_name: pry!(item.get_name()).to_string().unwrap_or_default(),
            item_rarity: pry!(item.get_rarity()),
            item_level: item.get_level(),
            quantity,
        };

        let mut st = self.state.lock().unwrap();
        let slots = st.inventories.entry(player_id).or_default();
        slot_data.slot_index = slots.len() as u16;
        slots.push(slot_data.clone());

        let mut r = results.get();
        build_inventory_slot(&mut r.reborrow().init_slot(), &slot_data);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn remove_item(
        &mut self,
        params: inventory_service::RemoveItemParams,
        mut results: inventory_service::RemoveItemResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let player_id = pry!(p.get_player()).get_id();
        let slot_index = p.get_slot_index();
        let quantity = p.get_quantity();

        let mut st = self.state.lock().unwrap();
        if let Some(slots) = st.inventories.get_mut(&player_id) {
            if let Some(slot) = slots.iter_mut().find(|s| s.slot_index == slot_index) {
                if slot.quantity >= quantity {
                    slot.quantity -= quantity;
                    if slot.quantity == 0 {
                        slots.retain(|s| s.slot_index != slot_index);
                    }
                    results.get().set_status(StatusCode::Ok);
                } else {
                    results.get().set_status(StatusCode::InvalidArgument);
                }
            } else {
                results.get().set_status(StatusCode::NotFound);
            }
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn start_trade(
        &mut self,
        params: inventory_service::StartTradeParams,
        mut results: inventory_service::StartTradeResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let initiator = pry!(p.get_initiator()).get_id();
        let target = pry!(p.get_target()).get_id();

        let session = TradeSessionImpl {
            _initiator: initiator,
            _target: target,
            state: Arc::new(Mutex::new(TradeSessionState {
                trade_state: TradeState::Proposing,
                offered_slots: Vec::new(),
                other_offered_slots: Vec::new(),
                accepted: false,
                other_accepted: false,
            })),
        };
        let client: trade_session::Client = capnp_rpc::new_client(session);
        let mut r = results.get();
        r.set_session(client);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn filter_by_rarity(
        &mut self,
        params: inventory_service::FilterByRarityParams,
        mut results: inventory_service::FilterByRarityResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let player_id = pry!(p.get_player()).get_id();
        let min_rarity = pry!(p.get_min_rarity());
        let min_rank = rarity_rank(min_rarity);

        let st = self.state.lock().unwrap();
        let filtered: Vec<_> = st
            .inventories
            .get(&player_id)
            .map(|slots| {
                slots
                    .iter()
                    .filter(|s| rarity_rank(s.item_rarity) >= min_rank)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        let mut list = results.get().init_items(filtered.len() as u32);
        for (i, s) in filtered.iter().enumerate() {
            build_inventory_slot(&mut list.reborrow().get(i as u32), s);
        }
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// TradeSession implementation
// ---------------------------------------------------------------------------

struct TradeSessionState {
    trade_state: TradeState,
    offered_slots: Vec<u16>,
    other_offered_slots: Vec<u16>,
    accepted: bool,
    other_accepted: bool,
}

struct TradeSessionImpl {
    _initiator: u64,
    _target: u64,
    state: Arc<Mutex<TradeSessionState>>,
}

fn build_trade_offer(
    builder: &mut crate::inventory_capnp::trade_offer::Builder<'_>,
    slots: &[u16],
    accepted: bool,
) {
    let mut items = builder.reborrow().init_offered_items(slots.len() as u32);
    for (i, &slot) in slots.iter().enumerate() {
        let mut s = items.reborrow().get(i as u32);
        s.set_slot_index(slot);
        s.set_quantity(1);
    }
    builder.set_accepted(accepted);
}

impl trade_session::Server for TradeSessionImpl {
    fn offer_items(
        &mut self,
        params: trade_session::OfferItemsParams,
        mut results: trade_session::OfferItemsResults,
    ) -> Promise<(), capnp::Error> {
        let slots_reader = pry!(pry!(params.get()).get_slots());
        let mut st = self.state.lock().unwrap();
        st.offered_slots.clear();
        for i in 0..slots_reader.len() {
            st.offered_slots.push(slots_reader.get(i));
        }
        let mut r = results.get();
        build_trade_offer(
            &mut r.reborrow().init_offer(),
            &st.offered_slots,
            st.accepted,
        );
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn remove_items(
        &mut self,
        params: trade_session::RemoveItemsParams,
        mut results: trade_session::RemoveItemsResults,
    ) -> Promise<(), capnp::Error> {
        let slots_reader = pry!(pry!(params.get()).get_slots());
        let to_remove: Vec<u16> = (0..slots_reader.len())
            .map(|i| slots_reader.get(i))
            .collect();
        let mut st = self.state.lock().unwrap();
        st.offered_slots.retain(|s| !to_remove.contains(s));
        let mut r = results.get();
        build_trade_offer(
            &mut r.reborrow().init_offer(),
            &st.offered_slots,
            st.accepted,
        );
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn accept(
        &mut self,
        _params: trade_session::AcceptParams,
        mut results: trade_session::AcceptResults,
    ) -> Promise<(), capnp::Error> {
        let mut st = self.state.lock().unwrap();
        st.accepted = true;
        if st.other_accepted {
            st.trade_state = TradeState::Accepted;
        }
        let mut r = results.get();
        r.set_state(st.trade_state);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn confirm(
        &mut self,
        _params: trade_session::ConfirmParams,
        mut results: trade_session::ConfirmResults,
    ) -> Promise<(), capnp::Error> {
        let mut st = self.state.lock().unwrap();
        st.trade_state = TradeState::Confirmed;
        let mut r = results.get();
        r.set_state(TradeState::Confirmed);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn cancel(
        &mut self,
        _params: trade_session::CancelParams,
        mut results: trade_session::CancelResults,
    ) -> Promise<(), capnp::Error> {
        let mut st = self.state.lock().unwrap();
        st.trade_state = TradeState::Cancelled;
        results.get().set_state(TradeState::Cancelled);
        Promise::ok(())
    }

    fn view_other_offer(
        &mut self,
        _params: trade_session::ViewOtherOfferParams,
        mut results: trade_session::ViewOtherOfferResults,
    ) -> Promise<(), capnp::Error> {
        let st = self.state.lock().unwrap();
        build_trade_offer(
            &mut results.get().init_offer(),
            &st.other_offered_slots,
            st.other_accepted,
        );
        Promise::ok(())
    }

    fn get_state(
        &mut self,
        _params: trade_session::GetStateParams,
        mut results: trade_session::GetStateResults,
    ) -> Promise<(), capnp::Error> {
        let st = self.state.lock().unwrap();
        results.get().set_state(st.trade_state);
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// Matchmaking implementation
// ---------------------------------------------------------------------------

struct MatchmakingState {
    next_ticket_id: u64,
    next_match_id: u64,
    queue: Vec<QueueEntry>,
    matches: HashMap<u64, MatchData>,
    results: HashMap<u64, MatchResultData>,
}

#[derive(Clone)]
struct QueueEntry {
    ticket_id: u64,
    mode: GameMode,
}

#[derive(Clone)]
struct MatchData {
    id: u64,
    mode: GameMode,
    state: MatchState,
    team_a: Vec<PlayerInfoData>,
    team_b: Vec<PlayerInfoData>,
    created_at: i64,
    ready_players: Vec<u64>,
}

#[derive(Clone)]
struct MatchResultData {
    match_id: u64,
    winning_team: u8,
    duration: u32,
    player_stats: Vec<PlayerMatchStatsData>,
}

#[derive(Clone)]
struct PlayerMatchStatsData {
    player: PlayerInfoData,
    kills: u32,
    deaths: u32,
    assists: u32,
    score: i32,
}

struct MatchmakingServiceImpl {
    state: Arc<Mutex<MatchmakingState>>,
}

impl MatchmakingServiceImpl {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MatchmakingState {
                next_ticket_id: 1,
                next_match_id: 1,
                queue: Vec::new(),
                matches: HashMap::new(),
                results: HashMap::new(),
            })),
        }
    }
}

fn build_match_info(
    builder: &mut crate::matchmaking_capnp::match_info::Builder<'_>,
    m: &MatchData,
) {
    builder.reborrow().init_id().set_id(m.id);
    builder.reborrow().set_mode(m.mode);
    builder.reborrow().set_state(m.state);
    let mut ta = builder.reborrow().init_team_a(m.team_a.len() as u32);
    for (i, p) in m.team_a.iter().enumerate() {
        build_player_info(&mut ta.reborrow().get(i as u32), p);
    }
    let mut tb = builder.reborrow().init_team_b(m.team_b.len() as u32);
    for (i, p) in m.team_b.iter().enumerate() {
        build_player_info(&mut tb.reborrow().get(i as u32), p);
    }
    builder
        .reborrow()
        .init_created_at()
        .set_unix_millis(m.created_at);
}

impl matchmaking_service::Server for MatchmakingServiceImpl {
    fn enqueue(
        &mut self,
        params: matchmaking_service::EnqueueParams,
        mut results: matchmaking_service::EnqueueResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let player = pry!(read_player_info(pry!(p.get_player())));
        let mode = pry!(p.get_mode());

        let mut st = self.state.lock().unwrap();
        let ticket_id = st.next_ticket_id;
        st.next_ticket_id += 1;
        st.queue.push(QueueEntry { ticket_id, mode });

        let mut r = results.get();
        let mut ticket = r.reborrow().init_ticket();
        ticket.set_ticket_id(ticket_id);
        build_player_info(&mut ticket.reborrow().init_player(), &player);
        ticket.reborrow().set_mode(mode);
        ticket
            .reborrow()
            .init_enqueued_at()
            .set_unix_millis(now_millis());
        ticket.set_estimated_wait_secs(30);
        r.set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn dequeue(
        &mut self,
        params: matchmaking_service::DequeueParams,
        mut results: matchmaking_service::DequeueResults,
    ) -> Promise<(), capnp::Error> {
        let ticket_id = pry!(params.get()).get_ticket_id();
        let mut st = self.state.lock().unwrap();
        let before = st.queue.len();
        st.queue.retain(|e| e.ticket_id != ticket_id);
        if st.queue.len() < before {
            results.get().set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn find_match(
        &mut self,
        params: matchmaking_service::FindMatchParams,
        mut results: matchmaking_service::FindMatchResults,
    ) -> Promise<(), capnp::Error> {
        let p = pry!(params.get());
        let player = pry!(read_player_info(pry!(p.get_player())));
        let mode = pry!(p.get_mode());

        let mut st = self.state.lock().unwrap();
        let match_id = st.next_match_id;
        st.next_match_id += 1;

        let opponent = PlayerInfoData {
            id: player.id + 1000,
            name: format!("Bot_{}", match_id),
            faction: Faction::Horde,
            level: player.level,
        };
        let match_data = MatchData {
            id: match_id,
            mode,
            state: MatchState::Ready,
            team_a: vec![player],
            team_b: vec![opponent],
            created_at: now_millis(),
            ready_players: Vec::new(),
        };
        st.matches.insert(match_id, match_data);

        let controller: match_controller::Client = capnp_rpc::new_client(MatchControllerImpl {
            match_id,
            state: self.state.clone(),
        });

        let mut r = results.get();
        r.set_controller(controller);
        r.reborrow().init_match_id().set_id(match_id);
        Promise::ok(())
    }

    fn get_queue_stats(
        &mut self,
        params: matchmaking_service::GetQueueStatsParams,
        mut results: matchmaking_service::GetQueueStatsResults,
    ) -> Promise<(), capnp::Error> {
        let mode = pry!(pry!(params.get()).get_mode());
        let st = self.state.lock().unwrap();
        let count = st.queue.iter().filter(|e| e.mode == mode).count() as u32;
        let mut r = results.get();
        r.set_players_in_queue(count);
        r.set_avg_wait_secs(if count > 0 { 30 } else { 0 });
        Promise::ok(())
    }

    fn get_match_result(
        &mut self,
        params: matchmaking_service::GetMatchResultParams,
        mut results: matchmaking_service::GetMatchResultResults,
    ) -> Promise<(), capnp::Error> {
        let match_id = pry!(pry!(params.get()).get_id()).get_id();
        let st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(rd) = st.results.get(&match_id) {
            let mut result = r.reborrow().init_result();
            result.reborrow().init_match_id().set_id(rd.match_id);
            result.reborrow().set_winning_team(rd.winning_team);
            result.reborrow().set_duration(rd.duration);
            let mut stats = result.init_player_stats(rd.player_stats.len() as u32);
            for (i, ps) in rd.player_stats.iter().enumerate() {
                let mut s = stats.reborrow().get(i as u32);
                build_player_info(&mut s.reborrow().init_player(), &ps.player);
                s.set_kills(ps.kills);
                s.set_deaths(ps.deaths);
                s.set_assists(ps.assists);
                s.set_score(ps.score);
            }
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// MatchController implementation
// ---------------------------------------------------------------------------

struct MatchControllerImpl {
    match_id: u64,
    state: Arc<Mutex<MatchmakingState>>,
}

impl match_controller::Server for MatchControllerImpl {
    fn get_info(
        &mut self,
        _params: match_controller::GetInfoParams,
        mut results: match_controller::GetInfoResults,
    ) -> Promise<(), capnp::Error> {
        let st = self.state.lock().unwrap();
        if let Some(m) = st.matches.get(&self.match_id) {
            build_match_info(&mut results.get().init_info(), m);
        }
        Promise::ok(())
    }

    fn signal_ready(
        &mut self,
        params: match_controller::SignalReadyParams,
        mut results: match_controller::SignalReadyResults,
    ) -> Promise<(), capnp::Error> {
        let player_id = pry!(pry!(params.get()).get_player()).get_id();
        let mut st = self.state.lock().unwrap();
        let mut r = results.get();
        if let Some(m) = st.matches.get_mut(&self.match_id) {
            if !m.ready_players.contains(&player_id) {
                m.ready_players.push(player_id);
            }
            let total = m.team_a.len() + m.team_b.len();
            let all_ready = m.ready_players.len() >= total;
            if all_ready {
                m.state = MatchState::InProgress;
            }
            r.set_all_ready(all_ready);
            r.set_status(StatusCode::Ok);
        } else {
            r.set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }

    fn report_result(
        &mut self,
        params: match_controller::ReportResultParams,
        mut results: match_controller::ReportResultResults,
    ) -> Promise<(), capnp::Error> {
        let result_reader = pry!(pry!(params.get()).get_result());
        let match_id = pry!(result_reader.get_match_id()).get_id();
        let ps_reader = pry!(result_reader.get_player_stats());
        let mut player_stats = Vec::new();
        for i in 0..ps_reader.len() {
            let ps = ps_reader.get(i);
            let p = pry!(ps.get_player());
            player_stats.push(PlayerMatchStatsData {
                player: pry!(read_player_info(p)),
                kills: ps.get_kills(),
                deaths: ps.get_deaths(),
                assists: ps.get_assists(),
                score: ps.get_score(),
            });
        }
        let rd = MatchResultData {
            match_id,
            winning_team: result_reader.get_winning_team(),
            duration: result_reader.get_duration(),
            player_stats,
        };
        let mut st = self.state.lock().unwrap();
        if let Some(m) = st.matches.get_mut(&self.match_id) {
            m.state = MatchState::Completed;
        }
        st.results.insert(match_id, rd);
        results.get().set_status(StatusCode::Ok);
        Promise::ok(())
    }

    fn cancel_match(
        &mut self,
        _params: match_controller::CancelMatchParams,
        mut results: match_controller::CancelMatchResults,
    ) -> Promise<(), capnp::Error> {
        let mut st = self.state.lock().unwrap();
        if let Some(m) = st.matches.get_mut(&self.match_id) {
            m.state = MatchState::Cancelled;
            results.get().set_status(StatusCode::Ok);
        } else {
            results.get().set_status(StatusCode::NotFound);
        }
        Promise::ok(())
    }
}

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------

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

    let listener = TcpListener::bind(addr).await?;
    println!("READY");
    let schema_name = normalize_schema_name(schema).to_string();

    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let schema_name = schema_name.clone();

        tokio::task::spawn_local(async move {
            let stream = stream.compat();
            let (reader, writer) = stream.split();

            let network = twoparty::VatNetwork::new(
                reader,
                writer,
                rpc_twoparty_capnp::Side::Server,
                Default::default(),
            );

            let bootstrap_client: capnp::capability::Client = match schema_name.as_str() {
                "game_world" => {
                    let client: game_world::Client = capnp_rpc::new_client(GameWorldImpl::new());
                    client.client
                }
                "chat" => {
                    let client: chat_service::Client =
                        capnp_rpc::new_client(ChatServiceImpl::new());
                    client.client
                }
                "inventory" => {
                    let client: inventory_service::Client =
                        capnp_rpc::new_client(InventoryServiceImpl::new());
                    client.client
                }
                "matchmaking" => {
                    let client: matchmaking_service::Client =
                        capnp_rpc::new_client(MatchmakingServiceImpl::new());
                    client.client
                }
                other => {
                    eprintln!("unknown schema: {}", other);
                    return;
                }
            };

            let rpc_system = RpcSystem::new(Box::new(network), Some(bootstrap_client));

            if let Err(e) = rpc_system.await {
                eprintln!("RPC error: {}", e);
            }
        });
    }
}
