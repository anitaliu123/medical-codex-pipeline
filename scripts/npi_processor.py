# scripts/npi_processor.py

import logging
import re
from pathlib import Path

import pandas as pd
from utils.common_functions import save_to_formats, now_utc_iso

# candidates for NPI column name
NPI_CANDIDATES = ["NPI", "npi", "Npi"]
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

def is_valid_npi(npi: str) -> bool:
    """Validate NPI (10 digits) using Luhn with the '80840' prefix."""
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

def _detect_columns(path: Path) -> tuple[str, str | None, list[str], list[str]]:
    """Pick NPI col + description sources; return also the usecols list."""
    header = pd.read_csv(path, dtype=str, nrows=0)
    header.columns = header.columns.str.strip()
    cols = list(header.columns)

    # find NPI column
    npi_col = next((c for c in NPI_CANDIDATES if c in cols), None)
    if not npi_col:
        # heuristic: most 10-digit values in a small sample
        sample = pd.read_csv(path, dtype=str, nrows=5000)
        sample.columns = sample.columns.str.strip()
        ten = re.compile(r"^\d{10}$")
        best, best_rate = None, 0.0
        for c in sample.columns:
            s = sample[c].astype(str).str.strip()
            r = s.str.fullmatch(ten).mean()
            if r > best_rate:
                best, best_rate = c, r
        npi_col = best

    if not npi_col:
        raise KeyError(f"NPI column not found. Headers seen: {cols[:40]}")

    org_col = next((c for c in ORG_COLS if c in cols), None)
    name_cols_present = [c for c in NAME_COLS if c in cols]

    usecols = [npi_col]
    if org_col:
        usecols.append(org_col)
    usecols.extend(name_cols_present)
    return npi_col, org_col, name_cols_present, usecols

def load_npi_data(filepath: str) -> pd.DataFrame:
    """Load only the columns we need to keep memory down."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    npi_col, org_col, name_cols_present, usecols = _detect_columns(path)
    df = pd.read_csv(path, dtype=str, usecols=usecols)
    df.columns = df.columns.str.strip()

    logging.info("Loaded NPI: %s rows using columns %s", len(df), usecols)
    return df

def clean_npi_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Return standardized columns: code, description, last_updated."""
    df = raw_data.copy()

    # NPI column from the reduced set
    npi_col = next((c for c in NPI_CANDIDATES if c in df.columns), None)
    if not npi_col:
        # fallback by heuristic again
        ten = re.compile(r"^\d{10}$")
        best, rate = None, 0.0
        for c in df.columns:
            r = df[c].astype(str).str.strip().str.fullmatch(ten).mean()
            if r > rate:
                best, rate = c, r
        npi_col = best
    if not npi_col:
        raise KeyError(f"NPI column not found in data. Have: {df.columns.tolist()[:40]}")

    # Build description: organization first; else join name parts if present
    org_col = next((c for c in ORG_COLS if c in df.columns), None)
    name_cols_present = [c for c in NAME_COLS if c in df.columns]
    if org_col:
        desc = df[org_col].astype(str)
    elif name_cols_present:
        parts = [df[c].fillna("") for c in name_cols_present]
        desc = pd.concat(parts, axis=1).apply(
            lambda row: " ".join([s for s in (str(x).strip() for x in row) if s]),
            axis=1,
        )
    else:
        desc = pd.Series([""] * len(df))

    out = pd.DataFrame({
        "code": df[npi_col].astype(str).str.strip(),
        "description": desc.astype(str).str.strip(),
    })

    # validate + clean
    out = out[out["code"].apply(is_valid_npi)]
    out = out[out["description"].ne("")]
    out = out.drop_duplicates("code")

    out["last_updated"] = now_utc_iso()
    logging.info("Cleaned NPI rows: %s", len(out))
    return out

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raw = load_npi_data("input/npidata_pfile_20050523-20250907.csv")
    clean = clean_npi_data(raw)
    save_to_formats(clean, "output/csv/npi_latest")
    logging.info("NPI processing completed -> output/csv/npi_latest.csv")

if __name__ == "__main__":
    main()

