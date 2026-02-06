package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	capnp "capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"

	"e2e-rpc-test/internal/chat"
	"e2e-rpc-test/internal/gametypes"
	"e2e-rpc-test/internal/gameworld"
	"e2e-rpc-test/internal/inventory"
	"e2e-rpc-test/internal/matchmaking"
)

// Suppress unused import warning for capnp.
var _ capnp.Client

var (
	testNum  int
	failures int
)

func tap(ok bool, desc string) {
	testNum++
	if ok {
		fmt.Printf("ok %d - %s\n", testNum, desc)
	} else {
		fmt.Printf("not ok %d - %s\n", testNum, desc)
		failures++
	}
}

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 4001, "server port")
	schema := flag.String("schema", "gameworld", "schema to test: gameworld, chat, inventory, matchmaking")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	rpcConn := rpc.NewConn(rpc.NewStreamTransport(conn), nil)
	defer rpcConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch *schema {
	case "gameworld":
		testGameWorld(ctx, rpcConn)
	case "chat":
		testChat(ctx, rpcConn)
	case "inventory":
		testInventory(ctx, rpcConn)
	case "matchmaking":
		testMatchmaking(ctx, rpcConn)
	default:
		log.Fatalf("unknown schema: %s", *schema)
	}

	fmt.Printf("1..%d\n", testNum)

	if failures > 0 {
		os.Exit(1)
	}
}

func testGameWorld(ctx context.Context, rpcConn *rpc.Conn) {
	client := gameworld.GameWorld(rpcConn.Bootstrap(ctx))

	// Test 1: Spawn an entity
	spawnFut, release := client.SpawnEntity(ctx, func(p gameworld.GameWorld_spawnEntity_Params) error {
		req, err := p.NewRequest()
		if err != nil {
			return err
		}
		req.SetKind(gameworld.EntityKind_player)
		_ = req.SetName("TestPlayer")
		pos, err := req.NewPosition()
		if err != nil {
			return err
		}
		pos.SetX(10.0)
		pos.SetY(20.0)
		pos.SetZ(30.0)
		req.SetFaction(gametypes.Faction_alliance)
		req.SetMaxHealth(100)
		return nil
	})
	defer release()

	spawnRes, err := spawnFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("spawnEntity: %v", err))
		return
	}
	tap(spawnRes.Status() == gametypes.StatusCode_ok, "spawnEntity returns ok status")

	entity, _ := spawnRes.Entity()
	eid, _ := entity.Id()
	entityID := eid.Id()
	tap(entityID > 0, "spawned entity has valid ID")
	name, _ := entity.Name()
	tap(name == "TestPlayer", "spawned entity has correct name")
	tap(entity.Health() == 100, "spawned entity has correct health")
	tap(entity.Alive(), "spawned entity is alive")

	// Test 2: Get the entity back
	getFut, releaseGet := client.GetEntity(ctx, func(p gameworld.GameWorld_getEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		return nil
	})
	defer releaseGet()

	getRes, err := getFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getEntity: %v", err))
		return
	}
	tap(getRes.Status() == gametypes.StatusCode_ok, "getEntity returns ok status")
	gotEntity, _ := getRes.Entity()
	gotName, _ := gotEntity.Name()
	tap(gotName == "TestPlayer", "getEntity returns correct entity")

	// Test 3: Move entity
	moveFut, releaseMove := client.MoveEntity(ctx, func(p gameworld.GameWorld_moveEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		pos, err := p.NewNewPosition()
		if err != nil {
			return err
		}
		pos.SetX(50.0)
		pos.SetY(60.0)
		pos.SetZ(70.0)
		return nil
	})
	defer releaseMove()

	moveRes, err := moveFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("moveEntity: %v", err))
		return
	}
	tap(moveRes.Status() == gametypes.StatusCode_ok, "moveEntity returns ok status")
	movedEntity, _ := moveRes.Entity()
	movedPos, _ := movedEntity.Position()
	tap(movedPos.X() == 50.0, "moveEntity updates X position")

	// Test 4: Damage entity
	dmgFut, releaseDmg := client.DamageEntity(ctx, func(p gameworld.GameWorld_damageEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		p.SetAmount(30)
		return nil
	})
	defer releaseDmg()

	dmgRes, err := dmgFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("damageEntity: %v", err))
		return
	}
	tap(dmgRes.Status() == gametypes.StatusCode_ok, "damageEntity returns ok status")
	dmgEntity, _ := dmgRes.Entity()
	tap(dmgEntity.Health() == 70, "damageEntity reduces health correctly")
	tap(!dmgRes.Killed(), "damageEntity does not kill (health > 0)")

	// Test 5: Kill entity with enough damage
	killFut, releaseKill := client.DamageEntity(ctx, func(p gameworld.GameWorld_damageEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		p.SetAmount(200)
		return nil
	})
	defer releaseKill()

	killRes, err := killFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("damageEntity (kill): %v", err))
		return
	}
	tap(killRes.Killed(), "damageEntity kills entity when damage exceeds health")
	killEntity, _ := killRes.Entity()
	tap(!killEntity.Alive(), "killed entity is not alive")
	tap(killEntity.Health() == 0, "killed entity has 0 health")

	// Test 6: Spawn another and query area
	spawnFut2, releaseSpawn2 := client.SpawnEntity(ctx, func(p gameworld.GameWorld_spawnEntity_Params) error {
		req, err := p.NewRequest()
		if err != nil {
			return err
		}
		req.SetKind(gameworld.EntityKind_npc)
		_ = req.SetName("NPC1")
		pos, _ := req.NewPosition()
		pos.SetX(1.0)
		pos.SetY(1.0)
		pos.SetZ(1.0)
		req.SetFaction(gametypes.Faction_neutral)
		return nil
	})
	defer releaseSpawn2()
	if _, err := spawnFut2.Struct(); err != nil {
		tap(false, fmt.Sprintf("spawnEntity (2): %v", err))
		return
	}

	queryFut, releaseQuery := client.QueryArea(ctx, func(p gameworld.GameWorld_queryArea_Params) error {
		q, err := p.NewQuery()
		if err != nil {
			return err
		}
		center, err := q.NewCenter()
		if err != nil {
			return err
		}
		center.SetX(0)
		center.SetY(0)
		center.SetZ(0)
		q.SetRadius(1000)
		q.Filter().SetAll()
		return nil
	})
	defer releaseQuery()

	queryRes, err := queryFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("queryArea: %v", err))
		return
	}
	tap(queryRes.Count() >= 1, "queryArea returns entities")

	// Test 7: Despawn entity
	despawnFut, releaseDespawn := client.DespawnEntity(ctx, func(p gameworld.GameWorld_despawnEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		return nil
	})
	defer releaseDespawn()

	despawnRes, err := despawnFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("despawnEntity: %v", err))
		return
	}
	tap(despawnRes.Status() == gametypes.StatusCode_ok, "despawnEntity returns ok status")

	// Test 8: Get despawned entity should return notFound
	getFut2, releaseGet2 := client.GetEntity(ctx, func(p gameworld.GameWorld_getEntity_Params) error {
		id, err := p.NewId()
		if err != nil {
			return err
		}
		id.SetId(entityID)
		return nil
	})
	defer releaseGet2()

	getRes2, err := getFut2.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getEntity (after despawn): %v", err))
		return
	}
	tap(getRes2.Status() == gametypes.StatusCode_notFound, "getEntity returns notFound for despawned entity")
}

func testChat(ctx context.Context, rpcConn *rpc.Conn) {
	client := chat.ChatService(rpcConn.Bootstrap(ctx))

	// Test 1: Create a room
	createFut, releaseCreate := client.CreateRoom(ctx, func(p chat.ChatService_createRoom_Params) error {
		_ = p.SetName("general")
		_ = p.SetTopic("General chat")
		return nil
	})
	defer releaseCreate()

	createRes, err := createFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("createRoom: %v", err))
		return
	}
	tap(createRes.Status() == gametypes.StatusCode_ok, "createRoom returns ok status")
	roomInfo, _ := createRes.Info()
	roomName, _ := roomInfo.Name()
	tap(roomName == "general", "created room has correct name")

	// Test 2: Create duplicate room
	dupFut, releaseDup := client.CreateRoom(ctx, func(p chat.ChatService_createRoom_Params) error {
		_ = p.SetName("general")
		_ = p.SetTopic("Duplicate")
		return nil
	})
	defer releaseDup()

	dupRes, err := dupFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("createRoom (dup): %v", err))
		return
	}
	tap(dupRes.Status() == gametypes.StatusCode_alreadyExists, "createRoom returns alreadyExists for duplicate")

	// Test 3: List rooms
	listFut, releaseList := client.ListRooms(ctx, nil)
	defer releaseList()

	listRes, err := listFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("listRooms: %v", err))
		return
	}
	rooms, _ := listRes.Rooms()
	tap(rooms.Len() >= 1, "listRooms returns at least one room")

	// Test 4: Join room and get a ChatRoom capability
	joinFut, releaseJoin := client.JoinRoom(ctx, func(p chat.ChatService_joinRoom_Params) error {
		_ = p.SetName("general")
		pi, err := p.NewPlayer()
		if err != nil {
			return err
		}
		pid, _ := pi.NewId()
		pid.SetId(42)
		_ = pi.SetName("TestPlayer")
		pi.SetFaction(gametypes.Faction_alliance)
		pi.SetLevel(60)
		return nil
	})
	defer releaseJoin()

	joinRes, err := joinFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("joinRoom: %v", err))
		return
	}
	tap(joinRes.Status() == gametypes.StatusCode_ok, "joinRoom returns ok status")

	room := joinRes.Room()
	tap(room.IsValid(), "joinRoom returns valid ChatRoom capability")

	// Test 5: Send a message through the ChatRoom capability
	sendFut, releaseSend := room.SendMessage(ctx, func(p chat.ChatRoom_sendMessage_Params) error {
		_ = p.SetContent("Hello, world!")
		return nil
	})
	defer releaseSend()

	sendRes, err := sendFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("sendMessage: %v", err))
		return
	}
	tap(sendRes.Status() == gametypes.StatusCode_ok, "sendMessage returns ok status")
	sentMsg, _ := sendRes.Message_()
	msgContent, _ := sentMsg.Content()
	tap(msgContent == "Hello, world!", "sendMessage echoes content")

	// Test 6: Send an emote
	emoteFut, releaseEmote := room.SendEmote(ctx, func(p chat.ChatRoom_sendEmote_Params) error {
		_ = p.SetContent("dances")
		return nil
	})
	defer releaseEmote()

	emoteRes, err := emoteFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("sendEmote: %v", err))
		return
	}
	tap(emoteRes.Status() == gametypes.StatusCode_ok, "sendEmote returns ok status")

	// Test 7: Get message history
	histFut, releaseHist := room.GetHistory(ctx, func(p chat.ChatRoom_getHistory_Params) error {
		p.SetLimit(10)
		return nil
	})
	defer releaseHist()

	histRes, err := histFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getHistory: %v", err))
		return
	}
	msgs, _ := histRes.Messages()
	tap(msgs.Len() >= 2, "getHistory returns sent messages")

	// Test 8: Get room info
	infoFut, releaseInfo := room.GetInfo(ctx, nil)
	defer releaseInfo()

	infoRes, err := infoFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getInfo: %v", err))
		return
	}
	ri, _ := infoRes.Info()
	riName, _ := ri.Name()
	tap(riName == "general", "getInfo returns correct room name")

	// Test 9: Whisper
	whisperFut, releaseWhisper := client.Whisper(ctx, func(p chat.ChatService_whisper_Params) error {
		from, err := p.NewFrom()
		if err != nil {
			return err
		}
		fid, _ := from.NewId()
		fid.SetId(42)
		_ = from.SetName("TestPlayer")
		from.SetFaction(gametypes.Faction_alliance)
		from.SetLevel(60)

		to, err := p.NewTo()
		if err != nil {
			return err
		}
		to.SetId(99)

		_ = p.SetContent("Secret message")
		return nil
	})
	defer releaseWhisper()

	whisperRes, err := whisperFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("whisper: %v", err))
		return
	}
	tap(whisperRes.Status() == gametypes.StatusCode_ok, "whisper returns ok status")
	whisperMsg, _ := whisperRes.Message_()
	tap(whisperMsg.Kind().Which() == chat.ChatMessage_kind_Which_whisper, "whisper message has whisper kind")

	// Test 10: Leave room
	leaveFut, releaseLeave := room.Leave(ctx, nil)
	defer releaseLeave()

	leaveRes, err := leaveFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("leave: %v", err))
		return
	}
	tap(leaveRes.Status() == gametypes.StatusCode_ok, "leave returns ok status")
}

func testInventory(ctx context.Context, rpcConn *rpc.Conn) {
	client := inventory.InventoryService(rpcConn.Bootstrap(ctx))

	// Test 1: Get empty inventory
	getInvFut, releaseGetInv := client.GetInventory(ctx, func(p inventory.InventoryService_getInventory_Params) error {
		player, err := p.NewPlayer()
		if err != nil {
			return err
		}
		player.SetId(1)
		return nil
	})
	defer releaseGetInv()

	getInvRes, err := getInvFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getInventory: %v", err))
		return
	}
	tap(getInvRes.Status() == gametypes.StatusCode_ok, "getInventory returns ok status")
	inv, _ := getInvRes.Inventory()
	tap(inv.UsedSlots() == 0, "initial inventory is empty")

	// Test 2: Add an item
	addFut, releaseAdd := client.AddItem(ctx, func(p inventory.InventoryService_addItem_Params) error {
		player, err := p.NewPlayer()
		if err != nil {
			return err
		}
		player.SetId(1)

		item, err := p.NewItem()
		if err != nil {
			return err
		}
		itemId, _ := item.NewId()
		itemId.SetId(100)
		_ = item.SetName("Sword of Testing")
		item.SetRarity(gametypes.Rarity_rare)
		item.SetLevel(42)

		p.SetQuantity(1)
		return nil
	})
	defer releaseAdd()

	addRes, err := addFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("addItem: %v", err))
		return
	}
	tap(addRes.Status() == gametypes.StatusCode_ok, "addItem returns ok status")
	slot, _ := addRes.Slot()
	slotItem, _ := slot.Item()
	slotItemName, _ := slotItem.Name()
	tap(slotItemName == "Sword of Testing", "addItem returns correct item name")
	tap(slotItem.Rarity() == gametypes.Rarity_rare, "addItem returns correct rarity")

	// Test 3: Add another item with different rarity
	addFut2, releaseAdd2 := client.AddItem(ctx, func(p inventory.InventoryService_addItem_Params) error {
		player, _ := p.NewPlayer()
		player.SetId(1)
		item, _ := p.NewItem()
		itemId, _ := item.NewId()
		itemId.SetId(101)
		_ = item.SetName("Shield of Legends")
		item.SetRarity(gametypes.Rarity_legendary)
		item.SetLevel(60)
		p.SetQuantity(1)
		return nil
	})
	defer releaseAdd2()

	addRes2, err := addFut2.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("addItem (2): %v", err))
		return
	}
	tap(addRes2.Status() == gametypes.StatusCode_ok, "addItem second item returns ok status")

	// Test 4: Get inventory now has 2 items
	getInvFut2, releaseGetInv2 := client.GetInventory(ctx, func(p inventory.InventoryService_getInventory_Params) error {
		player, _ := p.NewPlayer()
		player.SetId(1)
		return nil
	})
	defer releaseGetInv2()

	getInvRes2, err := getInvFut2.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getInventory (2): %v", err))
		return
	}
	inv2, _ := getInvRes2.Inventory()
	tap(inv2.UsedSlots() == 2, "inventory has 2 items after adding two")

	// Test 5: Filter by rarity
	filterFut, releaseFilter := client.FilterByRarity(ctx, func(p inventory.InventoryService_filterByRarity_Params) error {
		player, _ := p.NewPlayer()
		player.SetId(1)
		p.SetMinRarity(gametypes.Rarity_legendary)
		return nil
	})
	defer releaseFilter()

	filterRes, err := filterFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("filterByRarity: %v", err))
		return
	}
	filteredItems, _ := filterRes.Items()
	tap(filteredItems.Len() == 1, "filterByRarity returns only legendary items")

	// Test 6: Remove an item
	removeFut, releaseRemove := client.RemoveItem(ctx, func(p inventory.InventoryService_removeItem_Params) error {
		player, _ := p.NewPlayer()
		player.SetId(1)
		p.SetSlotIndex(0)
		p.SetQuantity(1)
		return nil
	})
	defer releaseRemove()

	removeRes, err := removeFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("removeItem: %v", err))
		return
	}
	tap(removeRes.Status() == gametypes.StatusCode_ok, "removeItem returns ok status")

	// Test 7: Start a trade (capability passing)
	tradeFut, releaseTrade := client.StartTrade(ctx, func(p inventory.InventoryService_startTrade_Params) error {
		init, _ := p.NewInitiator()
		init.SetId(1)
		target, _ := p.NewTarget()
		target.SetId(2)
		return nil
	})
	defer releaseTrade()

	tradeRes, err := tradeFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("startTrade: %v", err))
		return
	}
	tap(tradeRes.Status() == gametypes.StatusCode_ok, "startTrade returns ok status")
	session := tradeRes.Session()
	tap(session.IsValid(), "startTrade returns valid TradeSession capability")

	// Test 8: Get trade state
	stateFut, releaseState := session.GetState(ctx, nil)
	defer releaseState()

	stateRes, err := stateFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getState: %v", err))
		return
	}
	tap(stateRes.State() == inventory.TradeState_proposing, "initial trade state is proposing")

	// Test 9: Accept trade
	acceptFut, releaseAccept := session.Accept(ctx, nil)
	defer releaseAccept()

	acceptRes, err := acceptFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("accept: %v", err))
		return
	}
	tap(acceptRes.State() == inventory.TradeState_accepted, "accept changes state to accepted")

	// Test 10: Cancel trade
	cancelFut, releaseCancel := session.Cancel(ctx, nil)
	defer releaseCancel()

	cancelRes, err := cancelFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("cancel: %v", err))
		return
	}
	tap(cancelRes.State() == inventory.TradeState_cancelled, "cancel changes state to cancelled")
}

func testMatchmaking(ctx context.Context, rpcConn *rpc.Conn) {
	client := matchmaking.MatchmakingService(rpcConn.Bootstrap(ctx))

	// Test 1: Enqueue a player
	enqueueFut, releaseEnqueue := client.Enqueue(ctx, func(p matchmaking.MatchmakingService_enqueue_Params) error {
		player, err := p.NewPlayer()
		if err != nil {
			return err
		}
		pid, _ := player.NewId()
		pid.SetId(1)
		_ = player.SetName("Player1")
		player.SetFaction(gametypes.Faction_alliance)
		player.SetLevel(60)
		p.SetMode(matchmaking.GameMode_arena3v3)
		return nil
	})
	defer releaseEnqueue()

	enqueueRes, err := enqueueFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("enqueue: %v", err))
		return
	}
	tap(enqueueRes.Status() == gametypes.StatusCode_ok, "enqueue returns ok status")
	ticket, _ := enqueueRes.Ticket()
	ticketID := ticket.TicketId()
	tap(ticketID > 0, "enqueue returns valid ticket ID")
	tap(ticket.Mode() == matchmaking.GameMode_arena3v3, "ticket has correct game mode")

	// Test 2: Get queue stats
	statsFut, releaseStats := client.GetQueueStats(ctx, func(p matchmaking.MatchmakingService_getQueueStats_Params) error {
		p.SetMode(matchmaking.GameMode_arena3v3)
		return nil
	})
	defer releaseStats()

	statsRes, err := statsFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getQueueStats: %v", err))
		return
	}
	tap(statsRes.PlayersInQueue() >= 1, "queue has at least one player")

	// Test 3: Dequeue
	dequeueFut, releaseDequeue := client.Dequeue(ctx, func(p matchmaking.MatchmakingService_dequeue_Params) error {
		p.SetTicketId(ticketID)
		return nil
	})
	defer releaseDequeue()

	dequeueRes, err := dequeueFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("dequeue: %v", err))
		return
	}
	tap(dequeueRes.Status() == gametypes.StatusCode_ok, "dequeue returns ok status")

	// Test 4: Dequeue again (should be notFound)
	dequeueFut2, releaseDequeue2 := client.Dequeue(ctx, func(p matchmaking.MatchmakingService_dequeue_Params) error {
		p.SetTicketId(ticketID)
		return nil
	})
	defer releaseDequeue2()

	dequeueRes2, err := dequeueFut2.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("dequeue (2): %v", err))
		return
	}
	tap(dequeueRes2.Status() == gametypes.StatusCode_notFound, "dequeue returns notFound for removed ticket")

	// Test 5: FindMatch returns a MatchController capability
	findFut, releaseFind := client.FindMatch(ctx, func(p matchmaking.MatchmakingService_findMatch_Params) error {
		player, _ := p.NewPlayer()
		pid, _ := player.NewId()
		pid.SetId(1)
		_ = player.SetName("Player1")
		player.SetFaction(gametypes.Faction_alliance)
		player.SetLevel(60)
		p.SetMode(matchmaking.GameMode_duel)
		return nil
	})
	defer releaseFind()

	findRes, err := findFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("findMatch: %v", err))
		return
	}
	matchId, _ := findRes.MatchId()
	tap(matchId.Id() > 0, "findMatch returns valid match ID")

	controller := findRes.Controller()
	tap(controller.IsValid(), "findMatch returns valid MatchController capability")

	// Test 6: Get match info through controller
	infoFut, releaseInfo := controller.GetInfo(ctx, nil)
	defer releaseInfo()

	infoRes, err := infoFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getInfo: %v", err))
		return
	}
	info, _ := infoRes.Info()
	tap(info.Mode() == matchmaking.GameMode_duel, "match info has correct game mode")
	tap(info.State() == matchmaking.MatchState_ready || info.State() == matchmaking.MatchState_waiting, "match state is waiting or ready before signalReady")

	// Test 7: Signal ready
	readyFut, releaseReady := controller.SignalReady(ctx, func(p matchmaking.MatchController_signalReady_Params) error {
		player, _ := p.NewPlayer()
		player.SetId(1)
		return nil
	})
	defer releaseReady()

	readyRes, err := readyFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("signalReady: %v", err))
		return
	}
	tap(readyRes.Status() == gametypes.StatusCode_ok, "signalReady returns ok status")

	// Test 8: Cancel match
	cancelFut, releaseCancel := controller.CancelMatch(ctx, nil)
	defer releaseCancel()

	cancelRes, err := cancelFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("cancelMatch: %v", err))
		return
	}
	tap(cancelRes.Status() == gametypes.StatusCode_ok, "cancelMatch returns ok status")

	// Test 9: Verify match is cancelled via getInfo
	infoFut2, releaseInfo2 := controller.GetInfo(ctx, nil)
	defer releaseInfo2()

	infoRes2, err := infoFut2.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getInfo (after cancel): %v", err))
		return
	}
	info2, _ := infoRes2.Info()
	tap(info2.State() == matchmaking.MatchState_cancelled, "match state is cancelled after cancelMatch")

	// Test 10: GetMatchResult for unknown match
	resultFut, releaseResult := client.GetMatchResult(ctx, func(p matchmaking.MatchmakingService_getMatchResult_Params) error {
		mid, _ := p.NewId()
		mid.SetId(99999)
		return nil
	})
	defer releaseResult()

	resultRes, err := resultFut.Struct()
	if err != nil {
		tap(false, fmt.Sprintf("getMatchResult: %v", err))
		return
	}
	tap(resultRes.Status() == gametypes.StatusCode_notFound, "getMatchResult returns notFound for unknown match")
}
