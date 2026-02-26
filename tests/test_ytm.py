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


def test_enrich_ytm_removes_zero_realprice_artifact_and_uses_prevwaprice() -> None:
    bonds = [
        {
            "SECID": "BOND6",
            "RealPrice": 0,
            "PREVWAPRICE": 95.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 20,
            "COUPONPERCENT": 12.0,
            "MATDATE": "2028-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 13.7056
    assert bonds[0].get("RealPrice") in (None, "")


def test_enrich_ytm_removes_non_positive_realprice_even_without_ytm_inputs() -> None:
    bonds = [{"SECID": "BOND7", "RealPrice": "0,00", "MATDATE": "2029-01-01"}]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 0
    assert stats.skipped == 1
    assert "RealPrice" not in bonds[0]



def test_enrich_ytm_for_floater_uses_forecast_coupon_and_marks_row() -> None:
    bonds = [
        {
            "SECID": "FLOAT1",
            "CouponType": "Флоатер",
            "RealPrice": 98.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 0,
            "COUPONPERCENT": 5.0,
            "_INDEX_NAME": "RUONIA",
            "_INDEX_SPREAD": 1.2,
            "MATDATE": "2028-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 13.0808
    assert bonds[0]["_YTM_FORECAST"] is True


def test_enrich_ytm_for_floater_uses_last_year_cb_rate_for_long_horizon() -> None:
    class _Cfg:
        floater_cb_rate_current_year = 14.0
        floater_cb_rate_next_year = 8.5
        floater_cb_rate_plus_one_year = 8.0
        floater_ruonia_spread_from_cb_rate = -0.5

    bonds = [
        {
            "SECID": "FLOAT2",
            "CouponType": "Флоатер",
            "RealPrice": 100.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 0,
            "COUPONPERCENT": 0.0,
            "_INDEX_NAME": "RUONIA",
            "_INDEX_SPREAD": 0.5,
            "MATDATE": "2030-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1), config=_Cfg())

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 9.6239


def test_enrich_ytm_for_ofz_linker_uses_inflation_forecast_and_marks_row() -> None:
    bonds = [
        {
            "SECID": "SU52004RMFS8",
            "RealPrice": 95.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 10,
            "COUPONPERCENT": 2.5,
            "MATDATE": "2029-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1))

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 8.1348
    assert bonds[0]["_YTM_FORECAST"] is True


def test_enrich_ytm_for_ofz_linker_uses_last_inflation_year_for_long_horizon() -> None:
    class _Cfg:
        linker_inflation_current_year = 5.0
        linker_inflation_next_year = 4.0
        linker_inflation_plus_one_year = 4.0

    bonds = [
        {
            "SECID": "SU52003RMFS9",
            "RealPrice": 100.0,
            "FACEVALUE": 1000,
            "ACCRUEDINT": 0,
            "COUPONPERCENT": 2.5,
            "MATDATE": "2031-01-01",
        }
    ]

    stats = enrich_ytm(bonds, today=date(2026, 1, 1), config=_Cfg())

    assert stats.calculated == 1
    assert bonds[0]["YTM"] == 6.5989
