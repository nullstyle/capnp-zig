package servers

import (
	"context"
	"sync"
	"time"

	"e2e-rpc-test/internal/gametypes"
	"e2e-rpc-test/internal/matchmaking"
)

type MatchmakingServiceServer struct {
	mu           sync.Mutex
	nextTicket   uint64
	nextMatch    uint64
	queue        map[uint64]*queueEntry
	matches      map[uint64]*matchState
	matchResults map[uint64]*matchResultState
}

type queueEntry struct {
	ticketID   uint64
	playerName string
	playerID   uint64
	faction    gametypes.Faction
	level      uint16
	mode       matchmaking.GameMode
	enqueuedAt int64
}

type matchState struct {
	id        uint64
	mode      matchmaking.GameMode
	state     matchmaking.MatchState
	teamA     []playerRef
	teamB     []playerRef
	createdAt int64
	readySet  map[uint64]bool
}

type playerRef struct {
	name    string
	id      uint64
	faction gametypes.Faction
	level   uint16
}

type matchResultState struct {
	matchID     uint64
	winningTeam uint8
	duration    uint32
}

func NewMatchmakingServiceClient() matchmaking.MatchmakingService {
	s := &MatchmakingServiceServer{
		nextTicket:   1,
		nextMatch:    1,
		queue:        make(map[uint64]*queueEntry),
		matches:      make(map[uint64]*matchState),
		matchResults: make(map[uint64]*matchResultState),
	}
	return matchmaking.MatchmakingService_ServerToClient(s)
}

func (s *MatchmakingServiceServer) Enqueue(ctx context.Context, call matchmaking.MatchmakingService_enqueue) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}
	mode := args.Mode()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	pName, _ := player.Name()
	pId, _ := player.Id()
	now := time.Now().UnixMilli()

	s.mu.Lock()
	ticketID := s.nextTicket
	s.nextTicket++
	entry := &queueEntry{
		ticketID:   ticketID,
		playerName: pName,
		playerID:   pId.Id(),
		faction:    player.Faction(),
		level:      player.Level(),
		mode:       mode,
		enqueuedAt: now,
	}
	s.queue[ticketID] = entry
	s.mu.Unlock()

	ticket, err := res.NewTicket()
	if err != nil {
		return err
	}
	ticket.SetTicketId(ticketID)
	pi, _ := ticket.NewPlayer()
	pid, _ := pi.NewId()
	pid.SetId(pId.Id())
	_ = pi.SetName(pName)
	pi.SetFaction(player.Faction())
	pi.SetLevel(player.Level())
	ticket.SetMode(mode)
	ts, _ := ticket.NewEnqueuedAt()
	ts.SetUnixMillis(now)
	ticket.SetEstimatedWaitSecs(30)

	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (s *MatchmakingServiceServer) Dequeue(ctx context.Context, call matchmaking.MatchmakingService_dequeue) error {
	args := call.Args()
	ticketID := args.TicketId()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	_, ok := s.queue[ticketID]
	if ok {
		delete(s.queue, ticketID)
	}
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
	} else {
		res.SetStatus(gametypes.StatusCode_ok)
	}
	return nil
}

func (s *MatchmakingServiceServer) FindMatch(ctx context.Context, call matchmaking.MatchmakingService_findMatch) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}
	mode := args.Mode()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	pName, _ := player.Name()
	pId, _ := player.Id()
	now := time.Now().UnixMilli()

	s.mu.Lock()
	matchID := s.nextMatch
	s.nextMatch++

	m := &matchState{
		id:   matchID,
		mode: mode,
		state: matchmaking.MatchState_ready,
		teamA: []playerRef{{
			name:    pName,
			id:      pId.Id(),
			faction: player.Faction(),
			level:   player.Level(),
		}},
		teamB:     []playerRef{{name: "Opponent", id: 999, level: 10}},
		createdAt: now,
		readySet:  make(map[uint64]bool),
	}
	s.matches[matchID] = m
	s.mu.Unlock()

	controller := &MatchControllerServer{
		service: s,
		matchID: matchID,
	}
	controllerClient := matchmaking.MatchController_ServerToClient(controller)
	if err := res.SetController(controllerClient); err != nil {
		return err
	}
	mid, err := res.NewMatchId()
	if err != nil {
		return err
	}
	mid.SetId(matchID)
	return nil
}

func (s *MatchmakingServiceServer) GetQueueStats(ctx context.Context, call matchmaking.MatchmakingService_getQueueStats) error {
	args := call.Args()
	mode := args.Mode()

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	count := uint32(0)
	for _, e := range s.queue {
		if e.mode == mode {
			count++
		}
	}
	s.mu.Unlock()

	res.SetPlayersInQueue(count)
	res.SetAvgWaitSecs(30)
	return nil
}

func (s *MatchmakingServiceServer) GetMatchResult(ctx context.Context, call matchmaking.MatchmakingService_getMatchResult) error {
	args := call.Args()
	mid, err := args.Id()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	s.mu.Lock()
	mr, ok := s.matchResults[mid.Id()]
	s.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}

	result, err := res.NewResult()
	if err != nil {
		return err
	}
	rmid, _ := result.NewMatchId()
	rmid.SetId(mr.matchID)
	result.SetWinningTeam(mr.winningTeam)
	result.SetDuration(mr.duration)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

// MatchControllerServer implements the MatchController interface.
type MatchControllerServer struct {
	service *MatchmakingServiceServer
	matchID uint64
}

func (c *MatchControllerServer) GetInfo(ctx context.Context, call matchmaking.MatchController_getInfo) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	c.service.mu.Lock()
	m, ok := c.service.matches[c.matchID]
	c.service.mu.Unlock()

	if !ok {
		return nil
	}

	info, err := res.NewInfo()
	if err != nil {
		return err
	}
	fillMatchInfo(info, m)
	return nil
}

func (c *MatchControllerServer) SignalReady(ctx context.Context, call matchmaking.MatchController_signalReady) error {
	args := call.Args()
	player, err := args.Player()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	c.service.mu.Lock()
	m, ok := c.service.matches[c.matchID]
	if !ok {
		c.service.mu.Unlock()
		res.SetStatus(gametypes.StatusCode_notFound)
		return nil
	}
	m.readySet[player.Id()] = true
	totalPlayers := len(m.teamA) + len(m.teamB)
	allReady := len(m.readySet) >= totalPlayers
	if allReady {
		m.state = matchmaking.MatchState_inProgress
	}
	c.service.mu.Unlock()

	res.SetAllReady(allReady)
	res.SetStatus(gametypes.StatusCode_ok)
	return nil
}

func (c *MatchControllerServer) ReportResult(ctx context.Context, call matchmaking.MatchController_reportResult) error {
	args := call.Args()
	result, err := args.Result()
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	resultMatchId, _ := result.MatchId()

	c.service.mu.Lock()
	m, ok := c.service.matches[c.matchID]
	if ok {
		m.state = matchmaking.MatchState_completed
		c.service.matchResults[c.matchID] = &matchResultState{
			matchID:     resultMatchId.Id(),
			winningTeam: result.WinningTeam(),
			duration:    result.Duration(),
		}
	}
	c.service.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
	} else {
		res.SetStatus(gametypes.StatusCode_ok)
	}
	return nil
}

func (c *MatchControllerServer) CancelMatch(ctx context.Context, call matchmaking.MatchController_cancelMatch) error {
	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	c.service.mu.Lock()
	m, ok := c.service.matches[c.matchID]
	if ok {
		if m.state == matchmaking.MatchState_inProgress {
			c.service.mu.Unlock()
			res.SetStatus(gametypes.StatusCode_invalidArgument)
			return nil
		}
		m.state = matchmaking.MatchState_cancelled
	}
	c.service.mu.Unlock()

	if !ok {
		res.SetStatus(gametypes.StatusCode_notFound)
	} else {
		res.SetStatus(gametypes.StatusCode_ok)
	}
	return nil
}

func fillMatchInfo(info matchmaking.MatchInfo, m *matchState) {
	mid, _ := info.NewId()
	mid.SetId(m.id)
	info.SetMode(m.mode)
	info.SetState(m.state)

	teamA, _ := info.NewTeamA(int32(len(m.teamA)))
	for i, p := range m.teamA {
		pi := teamA.At(i)
		pid, _ := pi.NewId()
		pid.SetId(p.id)
		_ = pi.SetName(p.name)
		pi.SetFaction(p.faction)
		pi.SetLevel(p.level)
	}

	teamB, _ := info.NewTeamB(int32(len(m.teamB)))
	for i, p := range m.teamB {
		pi := teamB.At(i)
		pid, _ := pi.NewId()
		pid.SetId(p.id)
		_ = pi.SetName(p.name)
		pi.SetFaction(p.faction)
		pi.SetLevel(p.level)
	}

	ts, _ := info.NewCreatedAt()
	ts.SetUnixMillis(m.createdAt)
}

var _ matchmaking.MatchmakingService_Server = (*MatchmakingServiceServer)(nil)
var _ matchmaking.MatchController_Server = (*MatchControllerServer)(nil)
