package servers

import (
	"context"
	"sync"
	"time"

	capnp "capnproto.org/go/capnp/v3"

	"e2e-rpc-test/internal/chat"
	"e2e-rpc-test/internal/gametypes"
)

type ChatServiceServer struct {
	mu       sync.Mutex
	rooms    map[string]*chatRoom
	nextRoom uint64
}

type chatRoom struct {
	id       uint64
	name     string
	topic    string
	members  []gametypes.PlayerInfo
	messages []chatMsg
}

type chatMsg struct {
	senderName    string
	senderID      uint64
	senderFaction gametypes.Faction
	senderLevel   uint16
	content       string
	timestamp     int64
	isEmote       bool
	isSystem      bool
	isWhisper     bool
	whisperTarget uint64
}

func NewChatServiceClient() chat.ChatService {
	s := &ChatServiceServer{
		rooms:    make(map[string]*chatRoom),
		nextRoom: 1,
	}
	return chat.ChatService_ServerToClient(s)
}

func (s *ChatServiceServer) CreateRoom(ctx context.Context, call chat.ChatService_createRoom) error {
	args := call.Args()
	name, _ := args.Name()
	topic, _ := args.Topic()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	if _, exists := s.rooms[name]; exists {
		s.mu.Unlock()
		res.SetStatus(gametypes.StatusCode_alreadyExists)
		return nil
	}

	id := s.nextRoom
	s.nextRoom++
	room := &chatRoom{
		id:    id,
		name:  name,
		topic: topic,
	}
	s.rooms[name] = room
	s.mu.Unlock()

	// Return a ChatRoom capability
	roomServer := &ChatRoomServer{
		service: s,
		room:    room,
	}
	roomClient := chat.ChatRoom_ServerToClient(roomServer)
	if err := res.SetRoom(roomClient); err != nil {
		return err
	}

	info, err := res.NewInfo()
	if err != nil {
		return err
	}
	fillRoomInfo(info, room)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *ChatServiceServer) JoinRoom(ctx context.Context, call chat.ChatService_joinRoom) error {
	args := call.Args()
	name, _ := args.Name()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	room, ok := s.rooms[name]
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}

	roomServer := &ChatRoomServer{
		service: s,
		room:    room,
	}
	roomClient := chat.ChatRoom_ServerToClient(roomServer)
	if err := res.SetRoom(roomClient); err != nil {
		return err
	}
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *ChatServiceServer) ListRooms(ctx context.Context, call chat.ChatService_listRooms) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	rooms := make([]*chatRoom, 0, len(s.rooms))
	for _, r := range s.rooms {
		rooms = append(rooms, r)
	}
	s.mu.Unlock()

	roomList, err := res.NewRooms(int32(len(rooms)))
	if err != nil {
		return err
	}
	for i, r := range rooms {
		fillRoomInfo(roomList.At(i), r)
	}
	return nil
}

func (s *ChatServiceServer) Whisper(ctx context.Context, call chat.ChatService_whisper) error {
	args := call.Args()
	from, err := args.From()
	if err != nil {
		return err
	}
	to, err := args.To()
	if err != nil {
		return err
	}
	content, _ := args.Content()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	fromName, _ := from.Name()
	fromId, _ := from.Id()

	msg, err := res.NewMessage_()
	if err != nil {
		return err
	}
	sender, _ := msg.NewSender()
	sid, _ := sender.NewId()
	sid.SetId(fromId.Id())
	_ = sender.SetName(fromName)
	sender.SetFaction(from.Faction())
	sender.SetLevel(from.Level())

	_ = msg.SetContent(content)
	ts, _ := msg.NewTimestamp()
	ts.SetUnixMillis(time.Now().UnixMilli())
	whisperTarget, _ := msg.Kind().NewWhisper()
	whisperTarget.SetId(to.Id())

	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

// ChatRoomServer implements the ChatRoom interface.
type ChatRoomServer struct {
	service *ChatServiceServer
	room    *chatRoom
}

func (s *ChatRoomServer) SendMessage(ctx context.Context, call chat.ChatRoom_sendMessage) error {
	args := call.Args()
	content, _ := args.Content()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	s.service.mu.Lock()
	m := chatMsg{
		senderName: "system",
		content:    content,
		timestamp:  now,
	}
	s.room.messages = append(s.room.messages, m)
	s.service.mu.Unlock()

	msg, err := res.NewMessage_()
	if err != nil {
		return err
	}
	sender, _ := msg.NewSender()
	_ = sender.SetName("system")
	_ = msg.SetContent(content)
	ts, _ := msg.NewTimestamp()
	ts.SetUnixMillis(now)
	msg.Kind().SetNormal()

	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *ChatRoomServer) SendEmote(ctx context.Context, call chat.ChatRoom_sendEmote) error {
	args := call.Args()
	content, _ := args.Content()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	s.service.mu.Lock()
	m := chatMsg{
		senderName: "system",
		content:    content,
		timestamp:  now,
		isEmote:    true,
	}
	s.room.messages = append(s.room.messages, m)
	s.service.mu.Unlock()

	msg, err := res.NewMessage_()
	if err != nil {
		return err
	}
	sender, _ := msg.NewSender()
	_ = sender.SetName("system")
	_ = msg.SetContent(content)
	ts, _ := msg.NewTimestamp()
	ts.SetUnixMillis(now)
	msg.Kind().SetEmote()

	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *ChatRoomServer) GetHistory(ctx context.Context, call chat.ChatRoom_getHistory) error {
	args := call.Args()
	limit := args.Limit()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.service.mu.Lock()
	msgs := s.room.messages
	if uint32(len(msgs)) > limit && limit > 0 {
		msgs = msgs[len(msgs)-int(limit):]
	}
	s.service.mu.Unlock()

	msgList, err := res.NewMessages(int32(len(msgs)))
	if err != nil {
		return err
	}
	for i, m := range msgs {
		cm := msgList.At(i)
		sender, _ := cm.NewSender()
		_ = sender.SetName(m.senderName)
		_ = cm.SetContent(m.content)
		ts, _ := cm.NewTimestamp()
		ts.SetUnixMillis(m.timestamp)
		if m.isEmote {
			cm.Kind().SetEmote()
		} else {
			cm.Kind().SetNormal()
		}
	}
	return nil
}

func (s *ChatRoomServer) GetInfo(ctx context.Context, call chat.ChatRoom_getInfo) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.service.mu.Lock()
	room := s.room
	s.service.mu.Unlock()

	info, err := res.NewInfo()
	if err != nil {
		return err
	}
	fillRoomInfo(info, room)
	return nil
}

func (s *ChatRoomServer) Leave(ctx context.Context, call chat.ChatRoom_leave) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func fillRoomInfo(info chat.RoomInfo, r *chatRoom) {
	rid, _ := info.NewId()
	rid.SetId(r.id)
	_ = info.SetName(r.name)
	info.SetMemberCount(uint32(len(r.members)))
	_ = info.SetTopic(r.topic)
}

var _ chat.ChatService_Server = (*ChatServiceServer)(nil)
var _ chat.ChatRoom_Server = (*ChatRoomServer)(nil)

// Keep capnp import used.
var _ capnp.Client
