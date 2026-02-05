from __future__ import annotations

from pathlib import Path
import sys

import capnp  # type: ignore


ROOT = Path(__file__).resolve().parent
SCHEMA = ROOT / "interop.capnp"


def main() -> int:
    args = sys.argv[1:]
    packed = False

    if not args:
        print("usage: verify_pycapnp.py [--packed] <message.bin>")
        return 2

    if args[0] == "--packed":
        packed = True
        args = args[1:]

    if not args:
        print("usage: verify_pycapnp.py [--packed] <message.bin>")
        return 2

    message_path = Path(args[0])
    if not message_path.exists():
        print(f"missing file: {message_path}")
        return 2

    schema = capnp.load(str(SCHEMA))
    with message_path.open("rb") as f:
        if packed:
            reader = capnp._PackedFdMessageReader(f)
        else:
            reader = capnp._StreamFdMessageReader(f)
        root = reader.get_root(schema.Widget)

    if root.id != 123:
        print(f"unexpected id: {root.id}")
        return 1
    if root.name != "widget":
        print(f"unexpected name: {root.name}")
        return 1
    if len(root.points) != 3:
        print(f"unexpected points length: {len(root.points)}")
        return 1

    if len(root.tags) != 2:
        print(f"unexpected tags length: {len(root.tags)}")
        return 1

    expected = [(1, 10), (2, 20), (3, 30)]
    for idx, (x, y) in enumerate(expected):
        point = root.points[idx]
        if point.x != x or point.y != y:
            print(f"unexpected point[{idx}]: ({point.x}, {point.y})")
            return 1

    expected_tags = ["alpha", "beta"]
    for idx, tag in enumerate(expected_tags):
        if root.tags[idx] != tag:
            print(f"unexpected tag[{idx}]: {root.tags[idx]}")
            return 1

    if bytes(root.bytes) != b"\x01\x02\x03\x04\x05":
        print(f"unexpected bytes: {bytes(root.bytes)!r}")
        return 1

    expected_u16s = [10, 20, 30]
    if list(root.u16s) != expected_u16s:
        print(f"unexpected u16s: {list(root.u16s)}")
        return 1

    expected_u32s = [1000, 2000]
    if list(root.u32s) != expected_u32s:
        print(f"unexpected u32s: {list(root.u32s)}")
        return 1

    expected_u64s = [123456789, 987654321]
    if list(root.u64s) != expected_u64s:
        print(f"unexpected u64s: {list(root.u64s)}")
        return 1

    expected_bools = [True, False, True, False, True]
    if list(root.bools) != expected_bools:
        print(f"unexpected bools: {list(root.bools)}")
        return 1

    expected_f32s = [1.25, 2.5, -3.75]
    for idx, value in enumerate(expected_f32s):
        if abs(root.f32s[idx] - value) > 1e-6:
            print(f"unexpected f32s[{idx}]: {root.f32s[idx]}")
            return 1

    expected_f64s = [1.125, -2.25]
    for idx, value in enumerate(expected_f64s):
        if abs(root.f64s[idx] - value) > 1e-12:
            print(f"unexpected f64s[{idx}]: {root.f64s[idx]}")
            return 1

    if len(root.u16Lists) != 2:
        print(f"unexpected u16Lists length: {len(root.u16Lists)}")
        return 1
    if list(root.u16Lists[0]) != [7, 8]:
        print(f"unexpected u16Lists[0]: {list(root.u16Lists[0])}")
        return 1
    if list(root.u16Lists[1]) != [9, 10, 11]:
        print(f"unexpected u16Lists[1]: {list(root.u16Lists[1])}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
