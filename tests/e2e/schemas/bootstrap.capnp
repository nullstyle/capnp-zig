@0xa1b2c3d4e5f60006;

using GameWorld = import "game_world.capnp".GameWorld;
using ChatService = import "chat.capnp".ChatService;
using InventoryService = import "inventory.capnp".InventoryService;
using MatchmakingService = import "matchmaking.capnp".MatchmakingService;

# Bootstrap interface that provides access to all game services.
# This is the root capability exported by the server.
interface Bootstrap {
  gameWorld @0 () -> (service :GameWorld);
  chatService @1 () -> (service :ChatService);
  inventoryService @2 () -> (service :InventoryService);
  matchmakingService @3 () -> (service :MatchmakingService);
}
