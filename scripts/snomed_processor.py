# scripts/snomed_processor.py

import logging
from pathlib import Path

import pandas as pd
from utils.common_functions import save_to_formats, now_utc_iso, validate_code_format


def load_snomed_data(filepath: str) -> pd.DataFrame:
    """Load SNOMED data from a TSV file."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    df = pd.read_csv(path, sep="\t", dtype=str, na_filter=False)
    df.columns = df.columns.str.strip()

    logging.info("Loaded SNOMED: %s rows, %s cols", len(df), len(df.columns))
    logging.info("Columns (first 20): %s", list(df.columns)[:20])
    return df


def clean_snomed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize SNOMED to (code, description, last_updated)."""
    df = raw_data.copy()

    # Tolerate capitalization differences (conceptId, term)
    lower_map = {c.lower(): c for c in df.columns}
    concept_col = lower_map.get("conceptid")
    term_col = lower_map.get("term")
    if not concept_col or not term_col:
        raise KeyError(f"Need conceptId and term. Have: {df.columns.tolist()[:40]}")

    # Optional filters: keep active English terms when present
    if "active" in df.columns:
        df = df[df["active"] == "1"]
    lang_col = next((c for c in df.columns if c.lower() == "languagecode"), None)
    if lang_col:
        df = df[df[lang_col].str.lower() == "en"]

    out = df[[concept_col, term_col]].copy().rename(
        columns={concept_col: "code", term_col: "description"}
    )

    # Clean
    out["code"] = out["code"].astype(str).str.strip()
    out["description"] = out["description"].astype(str).str.strip()

    # Validate SNOMED conceptId (numeric, 6–18 digits)
    mask = validate_code_format(out["code"], r"^\d{6,18}$")
    out = out[mask & out["description"].ne("")].drop_duplicates("code")

    # Timestamp
    out["last_updated"] = now_utc_iso()

    logging.info("Cleaned SNOMED: %s rows", len(out))
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Load raw data
    raw_data = load_snomed_data("input/sct2_Description_Full-en_US1000124_20250301.txt")

    # Clean and process
    clean_data = clean_snomed_data(raw_data)

    # Save outputs (latest CSV)
    save_to_formats(clean_data, "output/csv/snomed_us_latest")

    logging.info("SNOMED processing completed")


if __name__ == "__main__":
    main()
