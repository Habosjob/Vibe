from io import BytesIO

import pandas as pd

from vibe.data_sources.moex_iss import read_csv_with_fallbacks
from vibe.ingest.moex_bond_rates import _validate_and_cast


def test_validate_and_cast_rates_csv_fixture() -> None:
    csv_data = """SECID,SHORTNAME,LAST,YIELD,MATDATE
SU26212RMFS9,ОФЗ 26212,95.31,13.42,16.01.2028
RU000A0JX0J2,Корп Бонд,101.05,10.10,20.02.2030
"""
    df = pd.read_csv(BytesIO(csv_data.encode("utf-8")))

    result = _validate_and_cast(df)

    assert not result.empty
    assert {"SECID", "LAST", "YIELD"}.issubset(result.columns)
    assert pd.api.types.is_numeric_dtype(result["LAST"])
    assert pd.api.types.is_datetime64_any_dtype(result["MATDATE"])


def test_cp1251_csv_parsing_with_fallbacks() -> None:
    csv_data = """SECID,SHORTNAME,LAST,YIELD
SU26212RMFS9,Тест Бонд,95.31,13.42
"""
    csv_bytes = csv_data.encode("cp1251")

    df, encoding = read_csv_with_fallbacks(csv_bytes)

    assert encoding == "cp1251"
    assert df.loc[0, "SHORTNAME"] == "Тест Бонд"
