@0xebd79e79ca5c0cd6;

struct Entry {
  key     @0 :Text;
  value   @1 :Data;
  version @2 :UInt64;
}

struct WriteOp {
  key @0 :Text;
  union {
    put    @1 :Data;
    delete @2 :Void;
  }
}

struct WriteOpResult {
  key @0 :Text;
  union {
    put    @1 :Entry;
    delete @2 :Bool;
  }
}

struct BackupInfo {
  backupId  @0 :UInt32;
  timestamp @1 :Int64;
  size      @2 :UInt64;
  numFiles  @3 :UInt32;
}

interface KvClientNotifier {
  keysChanged @0 (changes :List(WriteOpResult)) -> ();
  stateResetRequired @1 (restoredBackupId :UInt32, nextVersion :UInt64) -> ();
}

interface KvStore {
  get        @0 (key :Text)                   -> (entry :Entry, found :Bool);
  writeBatch @1 (ops :List(WriteOp))          -> (results :List(WriteOpResult), applied :UInt32, nextVersion :UInt64);
  list       @2 (prefix :Text, limit :UInt32) -> (entries :List(Entry));
  subscribe  @3 (notifier :KvClientNotifier)  -> ();
  setWatchedKeys @4 (keys :List(Text))        -> ();
  createBackup @5 (flushBeforeBackup :Bool) -> (backup :BackupInfo, backupCount :UInt32);
  listBackups @6 () -> (backups :List(BackupInfo));
  restoreFromBackup @7 (backupId :UInt32, keepLogFiles :Bool) -> (restoredBackupId :UInt32, nextVersion :UInt64);
}
