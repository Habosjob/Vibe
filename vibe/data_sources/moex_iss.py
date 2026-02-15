from __future__ import annotations

import logging
import re
from io import StringIO

import csv

import pandas as pd

from vibe.utils.http import get_with_retries

logger = logging.getLogger(__name__)


def _decode_csv_bytes(csv_bytes: bytes) -> tuple[str, str]:
    encodings = ("utf-8", "utf-8-sig", "cp1251")
    for encoding in encodings:
        try:
            return csv_bytes.decode(encoding), encoding
        except UnicodeDecodeError:
            continue

    logger.warning("Falling back to latin1 while decoding MOEX rates CSV.")
    return csv_bytes.decode("latin1"), "latin1"


def _detect_header_line(lines: list[str], *, max_scan_lines: int = 50) -> tuple[int | None, str | None]:
    header_pattern = re.compile(r"[A-Za-z_]")
    best_candidate: tuple[int, str, int] | None = None

    for idx, line in enumerate(lines[:max_scan_lines]):
        fields_semicolon = line.count(";") + 1
        fields_comma = line.count(",") + 1
        if fields_semicolon >= fields_comma:
            sep = ";"
            field_count = fields_semicolon
        else:
            sep = ","
            field_count = fields_comma

        if field_count >= 5 and header_pattern.search(line):
            if best_candidate is None or field_count > best_candidate[2]:
                best_candidate = (idx, sep, field_count)

    if best_candidate is None:
        return None, None

    return best_candidate[0], best_candidate[1]


def _read_trimmed_csv(trimmed_text: str, sep: str) -> tuple[pd.DataFrame, int]:
    bad_lines_skipped = 0

    def _skip_bad_line(_: list[str]) -> None:
        nonlocal bad_lines_skipped
        bad_lines_skipped += 1
        return None

    df = pd.read_csv(
        StringIO(trimmed_text),
        engine="python",
        sep=sep,
        on_bad_lines=_skip_bad_line,
        quoting=csv.QUOTE_MINIMAL,
        dtype=str,
    )
    return df, bad_lines_skipped


def parse_rates_csv_bytes(csv_bytes: bytes) -> tuple[pd.DataFrame, str, str, int, int]:
    text, encoding = _decode_csv_bytes(csv_bytes)
    lines = text.lstrip("\ufeff").splitlines()

    header_line_index, sep = _detect_header_line(lines)
    parse_attempts: list[tuple[int, str]] = []

    if header_line_index is not None and sep is not None:
        parse_attempts.append((header_line_index, sep))
    else:
        logger.warning("Failed to detect MOEX rates CSV header line; using fallback parser attempts.")
        parse_attempts.extend([(0, ";"), (0, ",")])

    if parse_attempts[0][1] == ";":
        parse_attempts.extend([(parse_attempts[0][0], ",")])
    else:
        parse_attempts.extend([(parse_attempts[0][0], ";")])

    seen_attempts: set[tuple[int, str]] = set()
    for header_idx, candidate_sep in parse_attempts:
        attempt_key = (header_idx, candidate_sep)
        if attempt_key in seen_attempts:
            continue
        seen_attempts.add(attempt_key)

        trimmed = "\n".join(lines[header_idx:])
        df, bad_lines_skipped = _read_trimmed_csv(trimmed, candidate_sep)
        if df.shape[1] >= 5:
            logger.info(
                "Parsed MOEX rates CSV: encoding=%s sep=%r header_line_index=%s rows=%s cols=%s bad_lines_skipped=%s",
                encoding,
                candidate_sep,
                header_idx,
                len(df),
                len(df.columns),
                bad_lines_skipped,
            )
            return df, encoding, candidate_sep, header_idx, bad_lines_skipped

    raise ValueError("Failed to parse MOEX rates CSV into a tabular DataFrame with expected columns.")


class MOEXBondRatesClient:
    def __init__(self, *, timeout: int = 30, retries: int = 3):
        self.timeout = timeout
        self.retries = retries

    def fetch_rates_csv_bytes(self, url: str) -> bytes:
        response = get_with_retries(url, timeout=self.timeout, retries=self.retries)
        logger.info(
            "Downloaded MOEX rates CSV: status=%s bytes=%s elapsed=%.3fs",
            response.status_code,
            len(response.content),
            response.elapsed_seconds,
        )
        return response.content

    def fetch_rates_df(self, url: str) -> pd.DataFrame:
        content = self.fetch_rates_csv_bytes(url)
        df, encoding, sep, header_idx, bad_lines_skipped = parse_rates_csv_bytes(content)
        logger.info(
            "Parsed MOEX rates CSV into DataFrame with row_count=%s encoding=%s sep=%r header_line_index=%s bad_lines_skipped=%s",
            len(df),
            encoding,
            sep,
            header_idx,
            bad_lines_skipped,
        )
        return df

    def fetch_rates_df_from_bytes(self, csv_bytes: bytes) -> pd.DataFrame:
        df, encoding, sep, header_idx, bad_lines_skipped = parse_rates_csv_bytes(csv_bytes)
        logger.info(
            "Parsed MOEX rates CSV bytes into DataFrame with row_count=%s encoding=%s sep=%r header_line_index=%s bad_lines_skipped=%s",
            len(df),
            encoding,
            sep,
            header_idx,
            bad_lines_skipped,
        )
        return df
