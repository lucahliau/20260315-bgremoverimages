#!/usr/bin/env python3
"""Minimal rembg wrapper - bypasses CLI/gradio deps.
Usage: python3 rembg_helper.py input.jpg output.png
"""
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 3:
        print("Usage: rembg_helper.py input.jpg output.png", file=sys.stderr)
        sys.exit(1)

    inp, out = Path(sys.argv[1]), Path(sys.argv[2])

    if not inp.exists():
        print(f"Input not found: {inp}", file=sys.stderr)
        sys.exit(1)

    try:
        from PIL import Image
    except ImportError:
        print("Pillow not installed. Run: pip3 install Pillow", file=sys.stderr)
        sys.exit(1)

    try:
        from rembg import remove
    except ImportError:
        print("rembg not installed. Run: pip3 install rembg", file=sys.stderr)
        sys.exit(1)

    img = Image.open(inp)
    result = remove(img)

    # Resize if too large (max 2048px on longest side)
    max_side = 2048
    w, h = result.size
    if max(w, h) > max_side:
        scale = max_side / max(w, h)
        result = result.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    out.parent.mkdir(parents=True, exist_ok=True)
    result.save(str(out), "PNG", optimize=True)
    print(f"Saved {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
