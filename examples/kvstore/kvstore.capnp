@0xebd79e79ca5c0cd6;

struct Entry {
  key     @0 :Text;
  value   @1 :Data;
  version @2 :UInt64;
}

interface KvStore {
  get    @0 (key :Text)                   -> (value :Data, found :Bool);
  set    @1 (key :Text, value :Data)      -> (entry :Entry);
  delete @2 (key :Text)                   -> (found :Bool);
  list   @3 (prefix :Text, limit :UInt32) -> (entries :List(Entry));
}
