# scripts/icd10cm_processor.py
import csv
import logging
import re
from pathlib import Path

import pandas as pd
from utils.common_functions import save_to_formats, now_utc_iso, validate_code_format


def load_icd10cm_data(filepath: str) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    # try TSV or CSV first
    try:
        df = pd.read_csv(path, sep="\t", dtype=str, engine="python")
    except Exception:
        df = pd.read_csv(path, sep=",", dtype=str, engine="python")

    # If it didn't parse (everything in one column), fall back to fixed-width parsing
    if df.shape[1] > 1:
        df.columns = df.columns.astype(str).str.strip()
        logging.info("Loaded ICD-10-CM (delimited): %s rows, %s cols", len(df), len(df.columns))
        logging.info("Columns (first 20): %s", list(df.columns)[:20])
        return df

    # fixed-width / space-padded order file
    # Example line you showed:
    # '00001 A00     0 Cholera                                                      Cholera'
    import re

    line_pat_long = re.compile(
        r"^\s*(\d+)\s+([A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?)\s+([01])\s+(.*?)\s{2,}(.*?)\s*$"
    )
    line_pat_short = re.compile(
        r"^\s*(\d+)\s+([A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?)\s+([01])\s+(.*\S)\s*$"
    )

    rows = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        for raw in f:
            s = raw.rstrip("\n")
            if not s.strip():
                continue
            m = line_pat_long.match(s)
            if m:
                order, code, valid, short_desc, long_desc = m.groups()
            else:
                m2 = line_pat_short.match(s)
                if not m2:
                    continue
                order, code, valid, short_desc = m2.groups()
                long_desc = short_desc  # if there's only one desc, reuse it
            rows.append(
                {
                    "Order": order.strip(),
                    "ICD-10-CM Code": code.strip().upper(),
                    "Valid": valid.strip(),
                    "Short Description": short_desc.strip(),
                    "Long Description": long_desc.strip(),
                }
            )

    if not rows:
        raise RuntimeError(
            "Could not parse any rows from fixed-width ICD-10-CM order file. "
            "Check the file format or adjust the regex patterns."
        )

    df = pd.DataFrame(rows)
    logging.info("Loaded ICD-10-CM (fixed-width parsed): %s rows, %s cols", len(df), len(df.columns))
    logging.info("Columns: %s", list(df.columns))
    return df


def clean_icd10cm_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize ICD-10-CM codes to (code, description, last_updated)."""
    df = raw_data.copy()

    # Pick columns: prefer common names; else auto-detect by regex + text length
    code_col = next(
        (c for c in ["ICD-10-CM Code", "ICD-10-CM", "Code", "code", "icd10cm"] if c in df.columns),
        None,
    )
    desc_col = next(
        (c for c in ["Long Description", "Description", "Title",
                     "Short Description", "short_description", "long_description"] if c in df.columns),
        None,
    )

    icd_pattern = r"^[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?$"

    if not code_col:
        # auto-detect the column with the highest fraction of ICD-looking codes
        best, rate = None, 0.0
        for c in df.columns:
            s = df[c].astype(str).str.strip().str.upper()
            m = s.str.fullmatch(icd_pattern).mean()
            if m > rate:
                best, rate = c, m
        code_col = best

    if not desc_col:
        # choose the most "texty" non-code column if no preferred header
        candidates = [c for c in df.columns if c != code_col]
        if candidates:
            desc_col = max(candidates, key=lambda c: df[c].astype(str).str.len().mean())

    if not code_col or not desc_col:
        raise KeyError(f"Need code/description columns. Have: {df.columns.tolist()[:40]}")

    small = df[[code_col, desc_col]].copy().rename(columns={code_col: "code", desc_col: "description"})

    # Clean
    small["code"] = small["code"].astype(str).str.strip().str.upper()
    small["description"] = small["description"].astype(str).str.strip()

    # Validate codes (no 'U' starter; optional dot + up to 4 chars)
    mask = validate_code_format(small["code"], icd_pattern)
    small = small[mask & small["description"].ne("")].drop_duplicates("code")

    # Timestamp
    small["last_updated"] = now_utc_iso()

    logging.info("Cleaned ICD-10-CM: %s rows", len(small))
    return small


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Load raw data
    raw_data = load_icd10cm_data("input/icd10cm_order_2025.txt")

    # Clean and process
    clean_data = clean_icd10cm_data(raw_data)

    # Save outputs (latest CSV)
    save_to_formats(clean_data, "output/csv/icd10cm_latest")

    logging.info("ICD-10-CM processing completed")


if __name__ == "__main__":
    main()
