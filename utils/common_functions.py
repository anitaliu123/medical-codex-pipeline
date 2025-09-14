# utils/ common_functions.py
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

def now_utc_iso() -> str:
    """UTC timestamp like 2025-09-12T14:30:00Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Optional alias (some scripts may call this)
def utcnow_iso() -> str:
    return now_utc_iso()

def validate_code_format(series: pd.Series, pattern: str) -> pd.Series:
    """Vectorized fullmatch for code validation; returns a boolean mask."""
    return series.astype(str).str.fullmatch(pattern).fillna(False)

def save_to_formats(df: pd.DataFrame, base_filename: str) -> None:
    """
    Save a DataFrame as CSV with standardized columns.
    Example: save_to_formats(df, "output/csv/icd10cm_latest")
    """
    path = Path(base_filename).with_suffix(".csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [c for c in ("code", "description", "last_updated") if c in df.columns]
    df[cols].to_csv(path, index=False)