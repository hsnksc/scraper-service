from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
VENDOR = ROOT / ".vendor"

if VENDOR.exists():
    vendor_path = str(VENDOR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)
