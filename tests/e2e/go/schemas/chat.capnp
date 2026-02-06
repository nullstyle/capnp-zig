@0xa1b2c3d4e5f60003;

using Go = import "/go.capnp";
$Go.package("chat");
$Go.import("e2e-rpc-test/internal/chat");

using import "game_types.capnp".PlayerId;
using import "game_types.capnp".Timestamp;
using import "game_types.capnp".PlayerInfo;
using import "game_types.capnp".StatusCode;

# Chat service: player messaging with rooms/channels.
# Exercises: text handling, nested structs, capability passing (ChatRoom).

struct RoomId {
  id @0 :UInt64;
}

struct ChatMessage {
  sender @0 :PlayerInfo;
  content @1 :Text;
  timestamp @2 :Timestamp;
  kind :union {
    normal @3 :Void;
    emote @4 :Void;
    system @5 :Void;
    whisper @6 :PlayerId;  # Target of the whisper.
  }
}

struct RoomInfo {
  id @0 :RoomId;
  name @1 :Text;
  memberCount @2 :UInt32;
  topic @3 :Text;
}

# A ChatRoom capability: once you have a reference, you can interact with it.
# Exercises capability passing: ChatService.joinRoom returns a ChatRoom capability.
interface ChatRoom {
  # Send a message to this room.
  sendMessage @0 (content :Text) -> (message :ChatMessage, status :StatusCode);

  # Send an emote to this room.
  sendEmote @1 (content :Text) -> (message :ChatMessage, status :StatusCode);

  # Get recent messages.
  getHistory @2 (limit :UInt32) -> (messages :List(ChatMessage));

  # Get info about this room.
  getInfo @3 () -> (info :RoomInfo);

  # Leave this room (invalidates the capability).
  leave @4 () -> (status :StatusCode);
}

interface ChatService {
  # Create a new chat room and return a capability to it.
  createRoom @0 (name :Text, topic :Text) -> (room :ChatRoom, info :RoomInfo, status :StatusCode);

  # Join an existing room by name, receiving a ChatRoom capability.
  joinRoom @1 (name :Text, player :PlayerInfo) -> (room :ChatRoom, status :StatusCode);

  # List available rooms.
  listRooms @2 () -> (rooms :List(RoomInfo));

  # Send a direct whisper to another player (no room needed).
  whisper @3 (from :PlayerInfo, to :PlayerId, content :Text) -> (message :ChatMessage, status :StatusCode);
}
