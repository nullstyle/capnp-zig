@0xf00dfeedbeef1234;

# Regression schema for BUG 1: the generator must emit enum values and method
# ordinals from the element's ORDINAL (its @N), which equals its index in the
# schema's ordinal-sorted list, NOT its declaration order (code_order).
#
# Declaration order here differs from ordinal order on purpose:
#   Color declares green(@1) before red(@0) before blue(@2)
#   Svc  declares beta(@1) before alpha(@0) before gamma(@2)

enum Color {
  green @1;
  red @0;
  blue @2;
}

interface Svc {
  beta @1 () -> ();
  alpha @0 () -> ();
  gamma @2 () -> ();
}
