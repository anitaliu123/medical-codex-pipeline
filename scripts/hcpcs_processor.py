# scripts/hcpcs_processor.py
import re
import pandas as pd
from utils.common_functions import save_to_formats, now_utc_iso

# Read raw TXT as lines so I will split on big spaces/tabs
rows = []
with open("input/HCPC2025_OCT_ANWEB_v3.txt", encoding="utf-8", errors="ignore") as f:
    for line in f:
        # expect code at start (A–V + 4 digits), then description
        m = re.match(r"^\s*([A-V]\d{4})\s+(.*)$", line.rstrip("\n"))
        if not m:
            continue
        code, rest = m.group(1), m.group(2)
        # description = first token before big gaps or tab
        desc = re.split(r"\s{2,}|\t", rest.strip())[0] if rest.strip() else ""
        if desc:
            rows.append([code, desc])

hcpc = pd.DataFrame(rows, columns=["code", "description"])

### Info
hcpc.info()
if len(hcpc) > 0:
    print(hcpc.iloc[0])

# simple validation: HCPCS pattern
hcpc = hcpc[hcpc["code"].str.fullmatch(r"^[A-V]\d{4}$").fillna(False)]
hcpc["description"] = hcpc["description"].astype(str).str.strip()
hcpc = hcpc[hcpc["description"].ne("")].drop_duplicates("code")

hcpc['last_updated'] = '2025-09-05'
hcpc["last_updated"] = now_utc_iso()

save_to_formats(hcpc, "output/csv/hcpcs_latest")
