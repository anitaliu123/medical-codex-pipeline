from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.common_functions import save_to_formats, now_utc_iso

import pandas as pd
import re
from utils.common_functions import save_to_formats, now_utc_iso

## Input/Loinc.csv (raw)
loinc = pd.read_csv("input/Loinc.csv", dtype=str, low_memory=False)
loinc.columns = loinc.columns.str.strip()

### Info to describe 
loinc.info()

### Strings
if "STATUS" in loinc.columns:
    print(loinc["STATUS"].value_counts(dropna=False))

### print first row
if len(loinc) > 0:
    print(loinc.iloc[0])

#### Check potential column names that we think we want to keep: LOINC_NUM, DefinitionDescription
loinc.LOINC_NUM
loinc.LONG_COMMON_NAME

list_cols = ['LOINC_NUM', 'LONG_COMMON_NAME']

loinc_small = loinc[['LOINC_NUM', 'LONG_COMMON_NAME']]
loinc_small = loinc[list_cols]

loinc_small['last_updated'] = '2025-09-03'

# loinc_small = loinc_small.rename(columns={})

loinc_small = loinc_small.rename(columns={
    'LOINC_NUM': 'code',
    'LONG_COMMON_NAME': 'description',
})

file_output_path = ('output/Loinc.csv')

loinc_small.to_csv('output/loinc_small.csv')

loinc_small.to_csv('output/loinc_small_noindex.csv', index=False)

# save
save_to_formats(loinc_small, "output/csv/loinc_latest")