# scripts/icd10who_processor.py

import re, pandas as pd
import xml.etree.ElementTree as ET
from zipfile import ZipFile
from datetime import datetime, timezone

def h(s): print(f"\n### {s}")

IN_PATH = "input/icd102019en.xml.zip"
OUT_PATH = "output/csv/icd10who_latest.csv"

icd_pat = re.compile(r"^[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?$")

def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[-1]

def remove_leading_code(txt: str) -> str:
    return re.sub(r'^[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?\b[:\s-]*', '', txt or '').strip()

def load_root(path: str):
    path_l = path.lower()
    if path_l.endswith(".zip"):
        with ZipFile(path) as zf:
            name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
            if not name:
                raise RuntimeError("No .xml file found inside the zip.")
            xml_bytes = zf.read(name)
        return ET.fromstring(xml_bytes)
    elif path_l.endswith(".xml"):
        return ET.parse(path).getroot()
    else:
        raise RuntimeError("Expected .xml or .zip containing an .xml")

root = load_root(IN_PATH)

# extracting
rows, DESC_TAGS = [], {"title","name","desc","label","definition","rubric","text","caption"}
for el in root.iter():
    code = None
    for k, v in el.attrib.items():
        v = (v or "").strip()
        if icd_pat.fullmatch(v) and strip_ns(k).lower() in {"code","id","codeid","icdcode"}:
            code = v.upper(); break
    if not code:
        for child in el:
            tag = strip_ns(child.tag).lower()
            if tag in {"title","name","desc","label"}:
                t = (child.text or "").strip()
                m = re.match(r'^([A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?)\b', t)
                if m: code = m.group(1).upper(); break
    if not code:
        continue

    desc = ""
    for child in el:
        tag = strip_ns(child.tag).lower()
        if tag in DESC_TAGS:
            t = (child.text or "").strip()
            if t:
                desc = remove_leading_code(t)
                if desc: break
    if not desc:
        desc = remove_leading_code((el.text or "").strip())
    if desc:
        rows.append({"code": code, "description": desc})

df = pd.DataFrame(rows).drop_duplicates("code")

## info to describe
df.info()
if not df.empty:
    print(df.iloc[0])

# validate and save
df = df[df["code"].str.fullmatch(icd_pat).fillna(False)]
df = df[df["description"].astype(str).str.strip().ne("")]
df["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
df['last_updated'] = '2025-09-03'
df.to_csv(OUT_PATH, index=False)
print(f"Saved → {OUT_PATH} ({len(df)} rows)")
