from __future__ import annotations

import pytest
import pandas as pd

from vibe.ingest.moex_bonds_endpoints_probe import _extract_top_of_book_from_marketdata


def test_extract_top_of_book_scans_rows_and_marketdata_table() -> None:
    frame = pd.DataFrame(
        {
            "__table": ["securities", "marketdata", "marketdata_yields"],
            "BESTBID": [None, None, 99.95],
            "BESTOFFER": [None, 100.05, None],
            "BIDDEPTH": [None, None, 10],
            "OFFERDEPTH": [None, 12, None],
        }
    )

    top = _extract_top_of_book_from_marketdata(frame)

    assert top is not None
    assert float(top["top_of_book_best_bid"]) == pytest.approx(99.95)
    assert float(top["top_of_book_best_offer"]) == pytest.approx(100.05)
    assert float(top["top_of_book_spread"]) == pytest.approx(0.10)
    assert float(top["top_of_book_bid_depth"]) == pytest.approx(10)
    assert float(top["top_of_book_offer_depth"]) == pytest.approx(12)


def test_extract_top_of_book_falls_back_to_whole_frame_when_marketdata_table_is_empty() -> None:
    frame = pd.DataFrame(
        {
            "__table": ["marketdata", "securities"],
            "BESTBID": [None, 99.95],
            "BESTOFFER": [None, 100.05],
            "NUMBIDS": [None, 4],
            "NUMOFFERS": [None, 3],
        }
    )

    top = _extract_top_of_book_from_marketdata(frame)

    assert top is not None
    assert float(top["top_of_book_best_bid"]) == pytest.approx(99.95)
    assert float(top["top_of_book_best_offer"]) == pytest.approx(100.05)
    assert float(top["top_of_book_spread"]) == pytest.approx(0.10)
    assert float(top["top_of_book_bid_depth"]) == pytest.approx(4)
    assert float(top["top_of_book_offer_depth"]) == pytest.approx(3)
