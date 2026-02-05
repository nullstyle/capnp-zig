from __future__ import annotations

import argparse
import json
import random
import struct
from pathlib import Path

import capnp  # type: ignore

ROOT = Path(__file__).resolve().parent
SCHEMA = ROOT / "interop.capnp"


def rand_string(rng: random.Random, length: int) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    return "".join(rng.choice(alphabet) for _ in range(length))


def rand_f32(rng: random.Random) -> float:
    value = rng.uniform(-1000.0, 1000.0)
    return struct.unpack("<f", struct.pack("<f", value))[0]


def rand_f64(rng: random.Random) -> float:
    return rng.uniform(-1_000_000.0, 1_000_000.0)


def build_message(seed: int) -> tuple[object, dict[str, object]]:
    rng = random.Random(seed)
    schema = capnp.load(str(SCHEMA))
    msg = schema.Widget.new_message()

    msg_id = rng.randrange(0, 1_000_000)
    name = rand_string(rng, rng.randrange(0, 10))

    points_len = rng.randrange(0, 5)
    points_data: list[list[int]] = []
    points = msg.init("points", points_len)
    for i in range(points_len):
        x = rng.randrange(0, 10_000)
        y = rng.randrange(0, 10_000)
        points[i].x = x
        points[i].y = y
        points_data.append([x, y])

    tags_len = rng.randrange(0, 5)
    tags_data: list[str] = []
    tags = msg.init("tags", tags_len)
    for i in range(tags_len):
        tag = rand_string(rng, rng.randrange(0, 8))
        tags[i] = tag
        tags_data.append(tag)

    bytes_len = rng.randrange(0, 8)
    bytes_data = bytes(rng.randrange(0, 256) for _ in range(bytes_len))

    u16s_len = rng.randrange(0, 6)
    u16s_data = [rng.randrange(0, 65536) for _ in range(u16s_len)]
    u16s = msg.init("u16s", u16s_len)
    for i, value in enumerate(u16s_data):
        u16s[i] = value

    u32s_len = rng.randrange(0, 6)
    u32s_data = [rng.randrange(0, 1_000_000) for _ in range(u32s_len)]
    u32s = msg.init("u32s", u32s_len)
    for i, value in enumerate(u32s_data):
        u32s[i] = value

    u64s_len = rng.randrange(0, 6)
    u64s_data = [rng.randrange(0, 1_000_000_000_000) for _ in range(u64s_len)]
    u64s = msg.init("u64s", u64s_len)
    for i, value in enumerate(u64s_data):
        u64s[i] = value

    bools_len = rng.randrange(0, 9)
    bools_data = [bool(rng.randrange(0, 2)) for _ in range(bools_len)]
    bools = msg.init("bools", bools_len)
    for i, value in enumerate(bools_data):
        bools[i] = value

    f32s_len = rng.randrange(0, 6)
    f32s_data = [rand_f32(rng) for _ in range(f32s_len)]
    f32s = msg.init("f32s", f32s_len)
    for i, value in enumerate(f32s_data):
        f32s[i] = value

    f64s_len = rng.randrange(0, 6)
    f64s_data = [rand_f64(rng) for _ in range(f64s_len)]
    f64s = msg.init("f64s", f64s_len)
    for i, value in enumerate(f64s_data):
        f64s[i] = value

    u16_lists_len = rng.randrange(0, 4)
    u16_lists_data: list[list[int]] = []
    u16_lists = msg.init("u16Lists", u16_lists_len)
    for i in range(u16_lists_len):
        inner_len = rng.randrange(0, 5)
        inner = [rng.randrange(0, 65536) for _ in range(inner_len)]
        u16_lists[i] = inner
        u16_lists_data.append(inner)

    msg.id = msg_id
    msg.name = name
    msg.bytes = bytes_data

    expected = {
        "seed": seed,
        "id": msg_id,
        "name": name,
        "points": points_data,
        "tags": tags_data,
        "bytes": list(bytes_data),
        "u16s": u16s_data,
        "u32s": u32s_data,
        "u64s": u64s_data,
        "bools": bools_data,
        "f32s": f32s_data,
        "f64s": f64s_data,
        "u16Lists": u16_lists_data,
    }

    return msg, expected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--out-bin", type=Path, required=True)
    parser.add_argument("--out-json", type=Path, required=True)
    parser.add_argument("--packed", action="store_true")
    args = parser.parse_args()

    msg, expected = build_message(args.seed)
    out_bytes = msg.to_bytes_packed() if args.packed else msg.to_bytes()

    args.out_bin.write_bytes(out_bytes)
    args.out_json.write_text(json.dumps(expected, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
