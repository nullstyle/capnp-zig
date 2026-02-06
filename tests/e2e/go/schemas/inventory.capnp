@0xa1b2c3d4e5f60004;

using Go = import "/go.capnp";
$Go.package("inventory");
$Go.import("e2e-rpc-test/internal/inventory");

using import "game_types.capnp".PlayerId;
using import "game_types.capnp".ItemId;
using import "game_types.capnp".Item;
using import "game_types.capnp".Rarity;
using import "game_types.capnp".StatusCode;

# Inventory service: item management and player-to-player trades.
# Exercises: capability passing (TradeSession), enums, lists of structs.

struct InventorySlot {
  slotIndex @0 :UInt16;
  item @1 :Item;
  quantity @2 :UInt32;
}

struct InventoryView {
  owner @0 :PlayerId;
  slots @1 :List(InventorySlot);
  capacity @2 :UInt16;
  usedSlots @3 :UInt16;
}

enum TradeState {
  proposing @0;
  accepted @1;
  confirmed @2;
  cancelled @3;
}

struct TradeOffer {
  offeredItems @0 :List(InventorySlot);
  accepted @1 :Bool;
}

# A TradeSession capability: represents an active trade between two players.
# Both parties receive a reference and interact through it.
interface TradeSession {
  # Offer items from your inventory into the trade.
  offerItems @0 (slots :List(UInt16)) -> (offer :TradeOffer, status :StatusCode);

  # Remove items from your offer.
  removeItems @1 (slots :List(UInt16)) -> (offer :TradeOffer, status :StatusCode);

  # Accept the current trade terms.
  accept @2 () -> (state :TradeState, status :StatusCode);

  # Confirm after both parties accept (finalizes the trade).
  confirm @3 () -> (state :TradeState, status :StatusCode);

  # Cancel the trade.
  cancel @4 () -> (state :TradeState);

  # View the other party's current offer.
  viewOtherOffer @5 () -> (offer :TradeOffer);

  # Get the current state of this trade.
  getState @6 () -> (state :TradeState);
}

interface InventoryService {
  # Get a player's full inventory.
  getInventory @0 (player :PlayerId) -> (inventory :InventoryView, status :StatusCode);

  # Add an item to a player's inventory (e.g. loot drop).
  addItem @1 (player :PlayerId, item :Item, quantity :UInt32) -> (slot :InventorySlot, status :StatusCode);

  # Remove an item from a player's inventory.
  removeItem @2 (player :PlayerId, slotIndex :UInt16, quantity :UInt32) -> (status :StatusCode);

  # Initiate a trade between two players. Returns a TradeSession capability.
  # Exercises capability passing: caller receives an interface as a return value.
  startTrade @3 (initiator :PlayerId, target :PlayerId) -> (session :TradeSession, status :StatusCode);

  # Filter items by rarity.
  filterByRarity @4 (player :PlayerId, minRarity :Rarity) -> (items :List(InventorySlot));
}
