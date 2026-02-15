import pandas as pd
import pytest

from vibe.data_sources.moex_iss import parse_rates_csv_bytes
from vibe.ingest.moex_bond_rates import _validate_and_cast


def test_validate_and_cast_rates_csv_fixture() -> None:
    csv_data = """SECID,SHORTNAME,LAST,YIELD,MATDATE
SU26212RMFS9,ОФЗ 26212,95.31,13.42,16.01.2028
RU000A0JX0J2,Корп Бонд,101.05,10.10,20.02.2030
"""
    df, *_ = parse_rates_csv_bytes(csv_data.encode("utf-8"))

    result = _validate_and_cast(df)

    assert not result.empty
    assert {"SECID", "LAST", "YIELD"}.issubset(result.columns)
    assert pd.api.types.is_numeric_dtype(result["LAST"])
    assert pd.api.types.is_datetime64_any_dtype(result["MATDATE"])


@pytest.fixture
def semicolon_preamble_csv_bytes() -> bytes:
    csv_data = """Отчет сформирован ISS
Данные по облигациям
Пояснение: разделитель ;
SECID;SHORTNAME;LAST;YIELD;MATDATE;NUMTRADES
SU26212RMFS9;ОФЗ 26212;95.31;13.42;16.01.2028;120
RU000A0JX0J2;Корп Бонд;101.05;10.10;20.02.2030;55
"""
    return csv_data.encode("cp1251")


def test_parse_rates_csv_skips_preamble_and_detects_header(semicolon_preamble_csv_bytes: bytes) -> None:
    df, encoding, sep, header_idx, bad_lines_skipped = parse_rates_csv_bytes(semicolon_preamble_csv_bytes)

    assert encoding == "cp1251"
    assert sep == ";"
    assert header_idx == 3
    assert bad_lines_skipped == 0
    assert list(df.columns) == ["SECID", "SHORTNAME", "LAST", "YIELD", "MATDATE", "NUMTRADES"]
    assert df.loc[0, "SHORTNAME"] == "ОФЗ 26212"


def test_parse_rates_csv_auto_detects_comma_separator() -> None:
    csv_data = """metadata line 1
metadata line 2
SECID,SHORTNAME,LAST,YIELD,MATDATE,NUMTRADES
SU26212RMFS9,OFZ 26212,95.31,13.42,16.01.2028,120
"""

    df, encoding, sep, header_idx, _ = parse_rates_csv_bytes(csv_data.encode("utf-8"))

    assert encoding == "utf-8"
    assert sep == ","
    assert header_idx == 2
    assert df.shape == (1, 6)


def test_validate_and_cast_rejects_single_column_dataframe() -> None:
    df = pd.DataFrame({"raw": ["value"]})

    with pytest.raises(ValueError, match="too few columns"):
        _validate_and_cast(df)
