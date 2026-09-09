@0xd7238dd877c2e941;
interface Parent(T) {
  echo @0 (value :T) -> (value :T);
}
interface Middle(U) extends(Parent(U)) {}
