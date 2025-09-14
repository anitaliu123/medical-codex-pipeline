import logging
import re
from pathlib import Path

import pandas as pd
from utils.common_functions import save_to_formats, now_utc_iso


# ---------- helpers ----------
def is_valid_npi(npi: str) -> bool:
    """Validate NPI using the Luhn algorithm with the '80840' prefix."""
    s = re.sub(r"\D", "", str(npi or ""))
    if len(s) != 10 or not s.isdigit():
        return False
    base = "80840" + s[:-1]
    total, dbl = 0, True
    for ch in reversed(base):
        d = int(ch)
        if dbl:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        dbl = not dbl
    check = (10 - (total % 10)) % 10
    return check == int(s[-1])


ORG_COLS = [
    "Provider Organization Name (Legal Business Name)",
    "Provider Organization Name",
    "provider_organization_name_legal_business_name",
    "Org Name",
]

NAME_COLS = [
    "Provider Name Prefix Text",
    "Provider First Name",
    "Provider Middle Name",
    "Provider Last Name (Legal Name)",
    "Provider Name Suffix Text",
]


def build_description(df: pd.DataFrame) -> pd.Series:
    """Prefer organization name; otherwise build a person name from parts."""
    for c in ORG_COLS:
        if c in df.columns:
            return df[c].astype(str)
    parts = []
    for c in NAME_COLS:
        parts.append(df[c].fillna("") if c in df.columns else pd.Series([""] * len(df)))
    return pd.concat(parts, axis=1).apply(
        lambda row: " ".join([s for s in (str(x).strip() for x in row) if s]),
        axis=1,
    )


# ---------- pipeline functions ----------
def load_npi_data(filepath: str) -> pd.DataFrame:
    """Load raw NPI CSV (as strings) and standardize column names."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")
    df = pd.read_csv(path, dtype=str)
    df.columns = df.columns.str.strip()
    logging.info("Loaded NPI: %s rows, %s cols", len(df), len(df.columns))
    logging.info("Columns (first 20): %s", list(df.columns)[:20])
    return df


def clean_npi_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Return standardized columns: code, description, last_updated."""
    df = raw_data.copy()
    npi_col = next((c for c in ["NPI", "npi", "Npi"] if c in df.columns), None)
    if not npi_col:
        raise KeyError(f"NPI column not found. Have: {df.columns.tolist()[:40]}")

    desc = build_description(df)

    out = pd.DataFrame({
        "code": df[npi_col].astype(str).str.strip(),
        "description": desc.astype(str).str.strip(),
    })

    # Validate, drop empties and duplicates
    out = out[out["code"].apply(is_valid_npi)]
    out = out[out["description"].ne("")]
    out = out.drop_duplicates("code")

    out["last_updated"] = now_utc_iso()
    logging.info("Cleaned NPI: %s rows", len(out))
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raw = load_npi_data("input/npidata_pfile_20050523-20250907.csv")
    clean = clean_npi_data(raw)
    save_to_formats(clean, "output/csv/npi_latest")
    logging.info("NPI processing completed. Saved -> output/csv/npi_latest.csv")


if __name__ == "__main__":
    main()