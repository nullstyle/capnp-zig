@0xbc7f0fb9836d8e11;

struct Profile {
  name @0 :Text;
}

struct OldVersion {
  id @0 :UInt64;
  label @1 :Text;
  profile @2 :Profile;
}

struct NewVersion {
  id @0 :UInt64;
  label @1 :Text;
  profile @2 :Profile;
  revision @3 :UInt32 = 42;
  note @4 :Text = "new-field-default";
  enabled @5 :Bool = true;
}
