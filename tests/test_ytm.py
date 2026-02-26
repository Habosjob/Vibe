from __future__ import annotations

from datetime import date

from moex_bond_screener.ytm import enrich_ytm


def test_enrich_ytm_for_coupon_bond_uses_realprice_and_accruedint() -> None:
    bonds = [
        {
            "SECID": "BOND1",
            "RealPrice": 95.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 20,
            "COUPONPERCENT": 12.0,
            "MATDATE": "2028-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert stats.skipped == 0
    assert bonds[0]["YTM"] == 13.7056


def test_enrich_ytm_for_zero_coupon_bond_when_coupon_less_than_one() -> None:
    bonds = [
        {
            "SECID": "BOND2",
            "RealPrice": 80,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 10,
            "COUPONPERCENT": 0.5,
            "MATDATE": "2029-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 7.2697


def test_enrich_ytm_uses_offerdate_before_matdate() -> None:
    bonds = [
        {
            "SECID": "BOND3",
            "PREVWAPRICE": 95.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 20,
            "COUPONPERCENT": 12.0,
            "OFFERDATE": "2027-01-01",
            "MATDATE": "2029-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 15.2284


def test_enrich_ytm_uses_prevwaprice_and_fallback_realprice_fields() -> None:
    bonds = [
        {
            "SECID": "BOND4",
            "PREVWAPRICE": "",
            "ASK": "",
            "LAST": "91.2",
            "BID": "90.0",
            "FACEVALUE": 1000,
            "ACCRUEDINT": 10,
            "COUPONPERCENT": 8.0,
            "MATDATE": "2028-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["RealPrice"] == 91.2
    assert bonds[0]["YTM"] == 12.3829


def test_enrich_ytm_skips_bond_without_required_data() -> None:
    bonds = [{"SECID": "BOND5", "PREVWAPRICE": "", "ASK": "", "LAST": "", "BID": "", "MATDATE": "2029-01-01"}]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 0
    assert stats.skipped == 1
    assert "YTM" not in bonds[0]
