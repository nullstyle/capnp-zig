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
  # Newly-added pointer fields with no default. An OldVersion message never
  # wrote these pointer slots, so they read back as null pointers. Per the
  # Cap'n Proto spec a null pointer means the field's default (empty), so
  # these must NOT error when read through the newer Reader.
  extraProfile @6 :Profile;
  tags @7 :List(Text);
  numbers @8 :List(UInt32);
  blob @9 :Data;
  children @10 :List(Profile);
}
