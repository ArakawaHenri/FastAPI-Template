from __future__ import annotations


def calc_new_map_size(
    map_size: int,
    used_bytes: int,
    projected: int,
    growth_factor: int,
    *,
    force: bool = False,
) -> int:
    normalized_growth_factor = max(growth_factor, 1)
    headroom = max(int(projected * 0.1), 16 * 4096)
    min_target = projected + headroom
    new_size = max(map_size * normalized_growth_factor, min_target)
    if force and new_size <= map_size:
        new_size = map_size + max(headroom, projected - used_bytes)
    return new_size
