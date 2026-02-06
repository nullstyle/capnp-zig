package servers

import (
	"context"
	"math"
	"sync"

	capnp "capnproto.org/go/capnp/v3"

	"e2e-rpc-test/internal/gametypes"
	"e2e-rpc-test/internal/gameworld"
)

type GameWorldServer struct {
	mu       sync.Mutex
	entities map[uint64]*entityState
	nextID   uint64
}

type entityState struct {
	id        uint64
	kind      gameworld.EntityKind
	name      string
	posX      float32
	posY      float32
	posZ      float32
	health    int32
	maxHealth int32
	faction   gametypes.Faction
	alive     bool
}

func NewGameWorldClient() gameworld.GameWorld {
	s := &GameWorldServer{
		entities: make(map[uint64]*entityState),
		nextID:   1,
	}
	return gameworld.GameWorld_ServerToClient(s)
}

func (s *GameWorldServer) SpawnEntity(ctx context.Context, call gameworld.GameWorld_spawnEntity) error {
	args := call.Args()
	req, err := args.Request()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	id := s.nextID
	s.nextID++

	name, _ := req.Name()
	pos, _ := req.Position()

	e := &entityState{
		id:        id,
		kind:      req.Kind(),
		name:      name,
		posX:      pos.X(),
		posY:      pos.Y(),
		posZ:      pos.Z(),
		health:    req.MaxHealth(),
		maxHealth: req.MaxHealth(),
		faction:   req.Faction(),
		alive:     true,
	}
	s.entities[id] = e
	s.mu.Unlock()

	ent, err := res.NewEntity()
	if err != nil {
		return err
	}
	fillEntity(ent, e)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *GameWorldServer) DespawnEntity(ctx context.Context, call gameworld.GameWorld_despawnEntity) error {
	args := call.Args()
	eid, err := args.Id()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entities[eid.Id()]; !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}
	delete(s.entities, eid.Id())
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *GameWorldServer) GetEntity(ctx context.Context, call gameworld.GameWorld_getEntity) error {
	args := call.Args()
	eid, err := args.Id()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	e, ok := s.entities[eid.Id()]
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}

	ent, err := res.NewEntity()
	if err != nil {
		return err
	}
	fillEntity(ent, e)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *GameWorldServer) MoveEntity(ctx context.Context, call gameworld.GameWorld_moveEntity) error {
	args := call.Args()
	eid, err := args.Id()
	if err != nil {
		return err
	}
	newPos, err := args.NewPosition()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	e, ok := s.entities[eid.Id()]
	if ok {
		e.posX = newPos.X()
		e.posY = newPos.Y()
		e.posZ = newPos.Z()
	}
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}

	ent, err := res.NewEntity()
	if err != nil {
		return err
	}
	fillEntity(ent, e)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *GameWorldServer) DamageEntity(ctx context.Context, call gameworld.GameWorld_damageEntity) error {
	args := call.Args()
	eid, err := args.Id()
	if err != nil {
		return err
	}
	amount := args.Amount()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	e, ok := s.entities[eid.Id()]
	killed := false
	if ok {
		e.health -= amount
		if e.health <= 0 {
			e.health = 0
			e.alive = false
			killed = true
		}
	}
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}

	ent, err := res.NewEntity()
	if err != nil {
		return err
	}
	fillEntity(ent, e)
	res.SetKilled(killed)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *GameWorldServer) QueryArea(ctx context.Context, call gameworld.GameWorld_queryArea) error {
	args := call.Args()
	query, err := args.Query()
	if err != nil {
		return err
	}

	center, err := query.Center()
	if err != nil {
		return err
	}
	radius := query.Radius()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	var matches []*entityState
	for _, e := range s.entities {
		dx := e.posX - center.X()
		dy := e.posY - center.Y()
		dz := e.posZ - center.Z()
		dist := float32(math.Sqrt(float64(dx*dx + dy*dy + dz*dz)))
		if dist > radius {
			continue
		}

		switch query.Filter().Which() {
		case gameworld.AreaQuery_filter_Which_all:
			matches = append(matches, e)
		case gameworld.AreaQuery_filter_Which_byKind:
			if e.kind == query.Filter().ByKind() {
				matches = append(matches, e)
			}
		case gameworld.AreaQuery_filter_Which_byFaction:
			if e.faction == query.Filter().ByFaction() {
				matches = append(matches, e)
			}
		}
	}
	s.mu.Unlock()

	entList, err := res.NewEntities(int32(len(matches)))
	if err != nil {
		return err
	}
	for i, e := range matches {
		fillEntity(entList.At(i), e)
	}
	res.SetCount(uint32(len(matches)))
	return nil
}

func fillEntity(ent gameworld.Entity, e *entityState) {
	eid, _ := ent.NewId()
	eid.SetId(e.id)
	ent.SetKind(e.kind)
	_ = ent.SetName(e.name)
	pos, _ := ent.NewPosition()
	pos.SetX(e.posX)
	pos.SetY(e.posY)
	pos.SetZ(e.posZ)
	ent.SetHealth(e.health)
	ent.SetMaxHealth(e.maxHealth)
	ent.SetFaction(e.faction)
	ent.SetAlive(e.alive)
}

// Ensure we implement the interface (compile-time check inserted below via generated code).
var _ gameworld.GameWorld_Server = (*GameWorldServer)(nil)

// Shutdown satisfies the server.Shutdowner interface if needed.
func (s *GameWorldServer) Shutdown() {
	// no-op
}

// We need a blank import for capnp to avoid unused import error if fillEntity
// or other helpers are refactored. The capnp import is used by the generated code
// interface but we reference it indirectly.
var _ capnp.Ptr
