package servers

import (
	"context"
	"sync"

	"e2e-rpc-test/internal/gametypes"
	"e2e-rpc-test/internal/inventory"
)

type InventoryServiceServer struct {
	mu          sync.Mutex
	inventories map[uint64]*playerInventory
}

type playerInventory struct {
	slots    []invSlot
	capacity uint16
}

type invSlot struct {
	slotIndex uint16
	itemID    uint64
	itemName  string
	rarity    gametypes.Rarity
	itemLevel uint16
	quantity  uint32
}

func NewInventoryServiceClient() inventory.InventoryService {
	s := &InventoryServiceServer{
		inventories: make(map[uint64]*playerInventory),
	}
	return inventory.InventoryService_ServerToClient(s)
}

func (s *InventoryServiceServer) getOrCreateInventory(playerID uint64) *playerInventory {
	inv, ok := s.inventories[playerID]
	if !ok {
		inv = &playerInventory{capacity: 20}
		s.inventories[playerID] = inv
	}
	return inv
}

func (s *InventoryServiceServer) GetInventory(ctx context.Context, call inventory.InventoryService_getInventory) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	inv := s.getOrCreateInventory(player.Id())
	slots := make([]invSlot, len(inv.slots))
	copy(slots, inv.slots)
	cap := inv.capacity
	s.mu.Unlock()

	view, err := res.NewInventory()
	if err != nil {
		return err
	}
	owner, _ := view.NewOwner()
	owner.SetId(player.Id())
	slotList, err := view.NewSlots(int32(len(slots)))
	if err != nil {
		return err
	}
	for i, sl := range slots {
		fillInvSlot(slotList.At(i), &sl)
	}
	view.SetCapacity(cap)
	view.SetUsedSlots(uint16(len(slots)))
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *InventoryServiceServer) AddItem(ctx context.Context, call inventory.InventoryService_addItem) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}
	item, err := args.Item()
	if err != nil {
		return err
	}
	quantity := args.Quantity()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	itemId, _ := item.Id()
	itemName, _ := item.Name()

	s.mu.Lock()
	inv := s.getOrCreateInventory(player.Id())
	if uint16(len(inv.slots)) >= inv.capacity {
		s.mu.Unlock()
		res.SetStatus(gametypes.StatusCode_resourceExhausted)
		return nil
	}
	slotIdx := uint16(len(inv.slots))
	sl := invSlot{
		slotIndex: slotIdx,
		itemID:    itemId.Id(),
		itemName:  itemName,
		rarity:    item.Rarity(),
		itemLevel: item.Level(),
		quantity:  quantity,
	}
	inv.slots = append(inv.slots, sl)
	s.mu.Unlock()

	slot, err := res.NewSlot()
	if err != nil {
		return err
	}
	fillInvSlot(slot, &sl)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *InventoryServiceServer) RemoveItem(ctx context.Context, call inventory.InventoryService_removeItem) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}
	slotIndex := args.SlotIndex()
	quantity := args.Quantity()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	inv := s.getOrCreateInventory(player.Id())
	found := false
	for i, sl := range inv.slots {
		if sl.slotIndex == slotIndex {
			found = true
			if quantity >= sl.quantity {
				inv.slots = append(inv.slots[:i], inv.slots[i+1:]...)
			} else {
				inv.slots[i].quantity -= quantity
			}
			break
		}
	}
	s.mu.Unlock()

	if !found {
		res.SetStatus(gametypes.StatusCode_notFound)
	} else {
		res.SetStatus(gametypes.StatusCode_ok)
	}
	return nil
}

func (s *InventoryServiceServer) StartTrade(ctx context.Context, call inventory.InventoryService_startTrade) error {
	args := call.Args()
	initiator, err := args.Initiator()
	if err != nil {
		return err
	}
	target, err := args.Target()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	ts := &TradeSessionServer{
		service:     s,
		initiatorID: initiator.Id(),
		targetID:    target.Id(),
		state:       inventory.TradeState_proposing,
	}
	tsClient := inventory.TradeSession_ServerToClient(ts)
	if err := res.SetSession(tsClient); err != nil {
		return err
	}
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *InventoryServiceServer) FilterByRarity(ctx context.Context, call inventory.InventoryService_filterByRarity) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}
	minRarity := args.MinRarity()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	inv := s.getOrCreateInventory(player.Id())
	var matches []invSlot
	for _, sl := range inv.slots {
		if sl.rarity >= minRarity {
			matches = append(matches, sl)
		}
	}
	s.mu.Unlock()

	items, err := res.NewItems(int32(len(matches)))
	if err != nil {
		return err
	}
	for i, sl := range matches {
		fillInvSlot(items.At(i), &sl)
	}
	return nil
}

// TradeSessionServer implements TradeSession.
type TradeSessionServer struct {
	service     *InventoryServiceServer
	initiatorID uint64
	targetID    uint64
	state       inventory.TradeState
	mu          sync.Mutex
	myOffer     []uint16
	otherOffer  []uint16
	myAccepted  bool
}

func (t *TradeSessionServer) OfferItems(ctx context.Context, call inventory.TradeSession_offerItems) error {
	args := call.Args()
	slotsList, err := args.Slots()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.myOffer = make([]uint16, slotsList.Len())
	for i := 0; i < slotsList.Len(); i++ {
		t.myOffer[i] = slotsList.At(i)
	}
	t.mu.Unlock()

	offer, err := res.NewOffer()
	if err != nil {
		return err
	}
	// Offer is simplified -- just set accepted to false
	offer.SetAccepted(false)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (t *TradeSessionServer) RemoveItems(ctx context.Context, call inventory.TradeSession_removeItems) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.myOffer = nil
	t.mu.Unlock()

	offer, err := res.NewOffer()
	if err != nil {
		return err
	}
	offer.SetAccepted(false)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (t *TradeSessionServer) Accept(ctx context.Context, call inventory.TradeSession_accept) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.myAccepted = true
	t.state = inventory.TradeState_accepted
	t.mu.Unlock()

	res.SetState(inventory.TradeState_accepted)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (t *TradeSessionServer) Confirm(ctx context.Context, call inventory.TradeSession_confirm) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.state = inventory.TradeState_confirmed
	t.mu.Unlock()

	res.SetState(inventory.TradeState_confirmed)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (t *TradeSessionServer) Cancel(ctx context.Context, call inventory.TradeSession_cancel) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.state = inventory.TradeState_cancelled
	t.mu.Unlock()

	res.SetState(inventory.TradeState_cancelled)
	return nil
}

func (t *TradeSessionServer) ViewOtherOffer(ctx context.Context, call inventory.TradeSession_viewOtherOffer) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	offer, err := res.NewOffer()
	if err != nil {
		return err
	}
	offer.SetAccepted(false)
	return nil
}

func (t *TradeSessionServer) GetState(ctx context.Context, call inventory.TradeSession_getState) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	t.mu.Lock()
	res.SetState(t.state)
	t.mu.Unlock()
	return nil
}

func fillInvSlot(slot inventory.InventorySlot, sl *invSlot) {
	slot.SetSlotIndex(sl.slotIndex)
	item, _ := slot.NewItem()
	itemId, _ := item.NewId()
	itemId.SetId(sl.itemID)
	_ = item.SetName(sl.itemName)
	item.SetRarity(sl.rarity)
	item.SetLevel(sl.itemLevel)
	slot.SetQuantity(sl.quantity)
}

var _ inventory.InventoryService_Server = (*InventoryServiceServer)(nil)
var _ inventory.TradeSession_Server = (*TradeSessionServer)(nil)
