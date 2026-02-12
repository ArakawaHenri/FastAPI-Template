from __future__ import annotations


def pct_encode_char(ch: str) -> str:
    return "".join(f"%{b:02X}" for b in ch.encode("utf-8"))


def is_windows_reserved_name(name: str) -> bool:
    if name in {"CON", "PRN", "AUX", "NUL"}:
        return True
    if len(name) == 4 and name[:3] in {"COM", "LPT"} and name[3].isdigit():
        return name[3] != "0"
    return False


def escape_leading_dots(name: str) -> str:
    i = 0
    while i < len(name) and name[i] == ".":
        i += 1
    if i == 0:
        return name
    return "%2E" * i + name[i:]


def encode_filename(name: str) -> str:
    # Encode Windows/Linux-invalid characters + '%' to avoid collisions.
    invalid_chars = set('<>:"/\\|?*%')

    parts: list[str] = []
    for ch in name:
        code = ord(ch)
        if ch in invalid_chars or code == 0 or code < 32 or code == 127:
            parts.append(pct_encode_char(ch))
        else:
            parts.append(ch)
    encoded = "".join(parts)

    # Encode trailing spaces/dots (invalid on Windows).
    i = len(encoded)
    while i > 0 and encoded[i - 1] in (" ", "."):
        i -= 1
    if i != len(encoded):
        trailing = encoded[i:]
        encoded = encoded[:i] + "".join(pct_encode_char(ch) for ch in trailing)

    # Encode Windows reserved device names (case-insensitive).
    base, dot, ext = encoded.partition(".")
    base_stripped = base.rstrip(" .")
    upper_base = base_stripped.upper()
    if is_windows_reserved_name(upper_base):
        encoded_base = "".join(pct_encode_char(ch) for ch in base)
        encoded = encoded_base + (dot + ext if dot else "")

    return encoded


def is_utf8(data: bytes) -> bool:
    try:
        data.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False
