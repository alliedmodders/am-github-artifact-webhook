#!/usr/bin/env python3
"""
Diagnostic tool to verify PDB GUIDs in a symbol store.

Reads the GUID+age directly from each PDB file and compares it to the
directory name that symstore used when publishing.  Any mismatch means the
symstore library computed the wrong hash; if everything matches, the PDBs
on the server simply came from a different build than the binary the
debugger is loading.

Usage:
    # Verify all PDBs in the symbol store
    uv run python verify_symbols.py /path/to/symbol_store

    # Read the GUID from a single PDB file
    uv run python verify_symbols.py --file /path/to/some.pdb
"""

import argparse
import struct
import math
import sys
from pathlib import Path


# ── Standalone PDB GUID reader (no symstore dependency) ─────────────

MSF7_SIGNATURE = b"Microsoft C/C++ MSF 7.00\r\n\x1aDS\0\0\0"


def _pages(size: int, page_size: int) -> int:
    return int(math.ceil(float(size) / page_size))


def read_pdb_guid(filepath: str | Path) -> tuple[str, int | None]:
    """
    Read GUID and age directly from a PDB file.

    Returns (guid_string, age).  guid_string is the 32-char uppercase hex
    representation used in symbol-server paths (e.g. "0E9B25ECC43B4DFF…").
    """
    with open(filepath, "rb") as f:
        sig = f.read(len(MSF7_SIGNATURE))
        if sig != MSF7_SIGNATURE:
            raise ValueError(f"Not an MSF 7.0 PDB file: {filepath}")

        page_size, _fpm, _pages_used, root_dir_size, _reserved = struct.unpack(
            "<IIIII", f.read(20)
        )

        # ── Load root stream page list (two levels of indirection) ──
        root_num_pages = _pages(root_dir_size, page_size)
        num_root_index_pages = _pages(root_num_pages * 4, page_size)

        root_index_pages = struct.unpack(
            f"<{num_root_index_pages}I", f.read(4 * num_root_index_pages)
        )

        root_page_data = b""
        for idx_page in root_index_pages:
            f.seek(idx_page * page_size)
            root_page_data += f.read(page_size)

        root_pages = struct.unpack(
            f"<{root_num_pages}I", root_page_data[: root_num_pages * 4]
        )

        # ── Helper: read bytes from the root stream ─────────────────
        def root_read(start: int, length: int) -> bytes:
            result = b""
            while length > 0:
                page_idx = start // page_size
                page_off = start % page_size
                f.seek(root_pages[page_idx] * page_size + page_off)
                chunk = min(length, page_size - page_off)
                result += f.read(chunk)
                start += chunk
                length -= chunk
            return result

        # ── Parse root stream: stream count + sizes ─────────────────
        num_streams = struct.unpack("<I", root_read(0, 4))[0]

        stream_sizes = []
        for i in range(num_streams):
            (sz,) = struct.unpack("<I", root_read(4 + i * 4, 4))
            stream_sizes.append(sz)

        # Compute page-list offset for each stream
        pages_offset = 4 + 4 * num_streams
        stream_page_lists: list[tuple[int, ...]] = []
        for sz in stream_sizes:
            n = _pages(sz, page_size)
            raw = root_read(pages_offset, n * 4)
            stream_page_lists.append(struct.unpack(f"<{n}I", raw))
            pages_offset += n * 4

        # ── Helper: read bytes from an arbitrary stream ─────────────
        def stream_read(stream_idx: int, start: int, length: int) -> bytes:
            s_pages = stream_page_lists[stream_idx]
            result = b""
            while length > 0:
                page_idx = start // page_size
                page_off = start % page_size
                f.seek(s_pages[page_idx] * page_size + page_off)
                chunk = min(length, page_size - page_off)
                result += f.read(chunk)
                start += chunk
                length -= chunk
            return result

        # ── Read GUID from PDB info stream (stream 1) ──────────────
        # Layout: Version(4) | Signature(4) | Age(4) | GUID(16)
        header = stream_read(1, 0, 28)
        _ver, _sig, _pdb_age, g1, g2, g3, g4 = struct.unpack("<IIIIHH8s", header)

        guid_str = f"{g1:08X}{g2:04X}{g3:04X}{g4.hex().upper()}"

        # ── Read age from DBI stream (stream 3) ────────────────────
        # The symstore library uses DBI age because PDB-stream age can
        # change when tools like pdbstr modify the file.
        age: int | None = None
        if len(stream_page_lists) > 3 and len(stream_page_lists[3]) > 0:
            dbi_header = stream_read(3, 0, 12)
            _, _, age = struct.unpack("<III", dbi_header)

        return guid_str, age


def format_hash(guid: str, age: int | None) -> str:
    if age is None:
        return guid
    return f"{guid}{age:x}"


# ── Commands ─────────────────────────────────────────────────────────


def verify_store(store_path: Path) -> int:
    """Walk a symbol store and verify every PDB's GUID against its path."""
    mismatches = 0
    checked = 0

    for pdb_dir in sorted(store_path.iterdir()):
        if not pdb_dir.is_dir() or pdb_dir.name == "000Admin":
            continue

        for hash_dir in sorted(pdb_dir.iterdir()):
            if not hash_dir.is_dir():
                continue

            pdb_file = hash_dir / pdb_dir.name
            if not pdb_file.exists() or not pdb_file.suffix.lower() == ".pdb":
                continue

            stored_hash = hash_dir.name
            try:
                guid, age = read_pdb_guid(pdb_file)
                computed_hash = format_hash(guid, age)
            except Exception as e:
                print(f"  ERROR  {pdb_dir.name}/{stored_hash}  →  {e}")
                mismatches += 1
                checked += 1
                continue

            checked += 1
            if computed_hash.upper() != stored_hash.upper():
                print(
                    f"  MISMATCH  {pdb_dir.name}\n"
                    f"    stored path:   {stored_hash}\n"
                    f"    actual GUID:   {computed_hash}"
                )
                mismatches += 1
            else:
                print(f"  OK  {pdb_dir.name}/{stored_hash}")

    print(f"\nChecked {checked} PDB(s), {mismatches} mismatch(es).")
    return 1 if mismatches else 0


def show_file(filepath: Path) -> int:
    """Print the GUID+age hash for a single PDB file."""
    try:
        guid, age = read_pdb_guid(filepath)
    except Exception as e:
        print(f"Error reading {filepath}: {e}", file=sys.stderr)
        return 1

    print(f"File: {filepath.name}")
    print(f"GUID: {guid}")
    print(f"Age:  {age}")
    print(f"Hash: {format_hash(guid, age)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify PDB GUIDs in a symbol store")
    parser.add_argument(
        "store_path",
        nargs="?",
        type=Path,
        help="Path to the symbol store root directory",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=Path,
        help="Read the GUID from a single PDB file",
    )
    args = parser.parse_args()

    if args.file:
        return show_file(args.file)
    elif args.store_path:
        return verify_store(args.store_path)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
