from __future__ import annotations

from pathlib import Path

import capnp  # type: ignore


ROOT = Path(__file__).resolve().parent
SCHEMA = ROOT / "interop.capnp"


def build_message(segment_size: int | None) -> capnp._MallocMessageBuilder:  # type: ignore[attr-defined]
    if segment_size is None:
        msg = capnp._MallocMessageBuilder()
    else:
        msg = capnp._MallocMessageBuilder(segment_size)

    schema = capnp.load(str(SCHEMA))
    root = msg.init_root(schema.Widget)
    root.id = 123
    root.name = "widget"

    points = root.init("points", 3)
    for i in range(3):
        points[i].x = i + 1
        points[i].y = (i + 1) * 10

    tags = root.init("tags", 2)
    tags[0] = "alpha"
    tags[1] = "beta"

    root.bytes = b"\x01\x02\x03\x04\x05"

    u16s = root.init("u16s", 3)
    u16s[0] = 10
    u16s[1] = 20
    u16s[2] = 30

    u32s = root.init("u32s", 2)
    u32s[0] = 1000
    u32s[1] = 2000

    u64s = root.init("u64s", 2)
    u64s[0] = 123456789
    u64s[1] = 987654321

    bools = root.init("bools", 5)
    bools[0] = True
    bools[1] = False
    bools[2] = True
    bools[3] = False
    bools[4] = True

    f32s = root.init("f32s", 3)
    f32s[0] = 1.25
    f32s[1] = 2.5
    f32s[2] = -3.75

    f64s = root.init("f64s", 2)
    f64s[0] = 1.125
    f64s[1] = -2.25

    u16_lists = root.init("u16Lists", 2)
    u16_lists[0] = [7, 8]
    u16_lists[1] = [9, 10, 11]

    return msg


def write_fixture(path: Path, segment_size: int | None, *, packed: bool = False) -> None:
    msg = build_message(segment_size)
    with path.open("wb") as f:
        if packed:
            capnp._write_packed_message_to_fd(f.fileno(), msg)
        else:
            capnp._write_message_to_fd(f.fileno(), msg)


def main() -> None:
    write_fixture(ROOT / "fixture_single.bin", None)
    # A tiny segment size forces far pointers and multi-segment output.
    write_fixture(ROOT / "fixture_far.bin", 1)
    write_fixture(ROOT / "fixture_single_packed.bin", None, packed=True)
    write_fixture(ROOT / "fixture_far_packed.bin", 1, packed=True)


if __name__ == "__main__":
    main()
