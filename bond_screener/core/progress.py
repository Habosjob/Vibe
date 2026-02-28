from __future__ import annotations

from typing import Iterable, TypeVar

from tqdm import tqdm

T = TypeVar("T")


def progress_iter(items: Iterable[T], desc: str, total: int | None = None) -> Iterable[T]:
    return tqdm(items, desc=desc, total=total, unit="item", dynamic_ncols=True)
