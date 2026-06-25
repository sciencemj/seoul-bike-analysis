"""Download OA-15248 (segment) semi-annual CSVs, aggregate to compact tables.

Source: 서울 열린데이터광장 OA-15248 (서울시 공공자전거 이용정보(월별)).
Raw files are large (~50-64MB each); we stream each, aggregate, then discard raw.
Outputs small tables under data/processed/ that the notebook reads.
"""
import os
import urllib.request

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "data", "raw")
PROC = os.path.join(ROOT, "data", "processed")
os.makedirs(RAW, exist_ok=True)
os.makedirs(PROC, exist_ok=True)

# seq -> label, 2023-01 .. 2025-12 (36 contiguous months, segment schema)
FILES = {38: "23H1", 41: "23H2", 43: "24H1", 44: "24H2", 45: "25H1", 46: "25H2"}
STD = ["ym", "sid", "sname", "usertype", "gender", "age",
       "rent", "exercise", "carbon", "dist_m", "dur_min"]


def download(seq, dest):
    url = (f"https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do"
           f"?infId=OA-15248&seq={seq}&useCache=false&infSeq=1")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)


def read_file(path):
    # 11 fixed columns, same order across years; quoting differs -> python engine + skip bad
    df = pd.read_csv(path, encoding="cp949", header=0, names=STD,
                     engine="python", on_bad_lines="skip")
    df["sid"] = pd.to_numeric(df["sid"].astype(str).str.strip().str.strip("'"),
                              errors="coerce").astype("Int64")
    for c in ["rent", "carbon", "dist_m", "dur_min"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["gender"] = (df["gender"].astype(str).str.strip().str.strip("'").str.upper()
                    .replace({"": "미상", "NAN": "미상", "NONE": "미상"}))
    df["age"] = df["age"].astype(str).str.strip().str.strip("'")
    df["usertype"] = df["usertype"].astype(str).str.strip().str.strip("'")
    df["ym"] = df["ym"].astype(str).str.strip().str.strip("'")
    return df.dropna(subset=["sid", "rent"])


sm, g_gender, g_age, g_user = [], [], [], []
for seq, lbl in FILES.items():
    dest = os.path.join(RAW, f"seg_{lbl}.csv")
    if not os.path.exists(dest):
        print(f"downloading seq {seq} ({lbl}) ...", flush=True)
        download(seq, dest)
    df = read_file(dest)
    print(f"  {lbl}: {len(df):,} rows, months {sorted(df.ym.unique())}")
    sm.append(df.groupby(["sid", "ym"], as_index=False)
              .agg(rent=("rent", "sum"), dist_m=("dist_m", "sum"),
                   dur_min=("dur_min", "sum"), carbon=("carbon", "sum")))
    g_gender.append(df.groupby(["ym", "gender"], as_index=False).rent.sum())
    g_age.append(df.groupby(["ym", "age"], as_index=False).rent.sum())
    g_user.append(df.groupby(["ym", "usertype"], as_index=False).rent.sum())
    os.remove(dest)  # discard raw to save disk

station_month = (pd.concat(sm).groupby(["sid", "ym"], as_index=False)
                 .sum(numeric_only=True))
pd.concat(g_gender).groupby(["ym", "gender"], as_index=False).rent.sum() \
    .to_csv(os.path.join(PROC, "seg_gender.csv"), index=False)
pd.concat(g_age).groupby(["ym", "age"], as_index=False).rent.sum() \
    .to_csv(os.path.join(PROC, "seg_age.csv"), index=False)
pd.concat(g_user).groupby(["ym", "usertype"], as_index=False).rent.sum() \
    .to_csv(os.path.join(PROC, "seg_usertype.csv"), index=False)
station_month.to_csv(os.path.join(PROC, "station_month.csv"), index=False)

print("station_month:", station_month.shape,
      "stations", station_month.sid.nunique(),
      "months", sorted(station_month.ym.unique())[0], "->",
      sorted(station_month.ym.unique())[-1])
