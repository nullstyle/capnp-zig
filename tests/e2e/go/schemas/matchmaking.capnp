@0xa1b2c3d4e5f60005;

using Go = import "/go.capnp";
$Go.package("matchmaking");
$Go.import("e2e-rpc-test/internal/matchmaking");

using import "game_types.capnp".PlayerId;
using import "game_types.capnp".PlayerInfo;
using import "game_types.capnp".Timestamp;
using import "game_types.capnp".StatusCode;

# Matchmaking service: queue management and match creation.
# Exercises: promise pipelining -- call methods on the MatchController
# returned by findMatch before the match has actually been found.

enum GameMode {
  duel @0;
  arena3v3 @1;
  arena5v5 @2;
  battleground @3;
}

enum MatchState {
  waiting @0;
  ready @1;
  inProgress @2;
  completed @3;
  cancelled @4;
}

struct MatchId {
  id @0 :UInt64;
}

struct QueueTicket {
  ticketId @0 :UInt64;
  player @1 :PlayerInfo;
  mode @2 :GameMode;
  enqueuedAt @3 :Timestamp;
  estimatedWaitSecs @4 :UInt32;
}

struct MatchInfo {
  id @0 :MatchId;
  mode @1 :GameMode;
  state @2 :MatchState;
  teamA @3 :List(PlayerInfo);
  teamB @4 :List(PlayerInfo);
  createdAt @5 :Timestamp;
}

struct MatchResult {
  matchId @0 :MatchId;
  winningTeam @1 :UInt8;  # 0 = team A, 1 = team B
  duration @2 :UInt32;    # Duration in seconds.
  playerStats @3 :List(PlayerMatchStats);
}

struct PlayerMatchStats {
  player @0 :PlayerInfo;
  kills @1 :UInt32;
  deaths @2 :UInt32;
  assists @3 :UInt32;
  score @4 :Int32;
}

# MatchController capability: manage a specific match.
# Returned by findMatch -- callers can pipeline calls on this
# (e.g., call getInfo or signalReady) before findMatch resolves.
interface MatchController {
  # Get info about this match.
  getInfo @0 () -> (info :MatchInfo);

  # Signal that the player is ready.
  signalReady @1 (player :PlayerId) -> (allReady :Bool, status :StatusCode);

  # Report the match result (only once match is in progress).
  reportResult @2 (result :MatchResult) -> (status :StatusCode);

  # Cancel the match (only if not yet in progress).
  cancelMatch @3 () -> (status :StatusCode);
}

interface MatchmakingService {
  # Join the matchmaking queue. Returns a ticket.
  enqueue @0 (player :PlayerInfo, mode :GameMode) -> (ticket :QueueTicket, status :StatusCode);

  # Leave the matchmaking queue.
  dequeue @1 (ticketId :UInt64) -> (status :StatusCode);

  # Find a match for the player. Returns a MatchController capability.
  # This is the key pipelining exercise: the caller can call
  # matchController.signalReady() on the returned capability before
  # findMatch itself has resolved.
  findMatch @2 (player :PlayerInfo, mode :GameMode) -> (controller :MatchController, matchId :MatchId);

  # Get the current queue status.
  getQueueStats @3 (mode :GameMode) -> (playersInQueue :UInt32, avgWaitSecs :UInt32);

  # Look up a past match by ID.
  getMatchResult @4 (id :MatchId) -> (result :MatchResult, status :StatusCode);
}
