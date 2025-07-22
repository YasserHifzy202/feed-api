
# -*- coding: utf-8 -*-
"""
Feed Comparator  v5.6.1  (duplicates + formula-code check)

• يكتشف تلقائيًا أي ملف Received وأيهما Delivered.
• يفحص التكرار: (Flock, Date, Qty) ⇒ "Duplicate row".
• يقارن كود العلف بين Female Feed Formula ID (Received) و Formula ID (Delivered)
  ⇒ "Formula mismatch".
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
from io import BytesIO
import re, uvicorn

app = FastAPI(title="Feed Comparator", version="5.6.1")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

TOL = 0.01  # kg tolerance

# ─── نماذج الإخراج ────────────────────────────────────────────────
class FeedRow(BaseModel):
    data: Dict[str, Any]
    has_error: bool
class Resp(BaseModel):
    received_data: List[FeedRow]
    delivered_data: List[FeedRow]

# ─── أدوات رأس العمود المرن ───────────────────────────────────────
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", s).lower()

def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """تحويل رؤوس الأعمدة غير المتطابقة إلى أسماء موحّدة."""
    mapping = {}
    for c in df.columns:
        n = _norm(str(c))
        if "flock" in n:
            mapping[c] = "Flock"
        elif "date" in n:
            mapping[c] = "Date"
        elif "feed" in n and "received" in n:
            mapping[c] = "Qty_Received"
        elif "relative" in n and "net" in n:
            mapping[c] = "Qty_Delivered"
        # الأكواد
        elif "female" in n and "formula" in n:
            mapping[c] = "Code_Received"
        elif ("formula" in n and "id" in n) or ("delivery" in n and "formula" in n):
            mapping[c] = "Code_Delivered"
    return df.rename(columns=mapping)

# ─── تحميل واكتشاف الرأس ─────────────────────────────────────────
def _find_header(raw: pd.DataFrame) -> int:
    for i in range(min(40, len(raw))):
        if raw.iloc[i].dropna().size and any(
            _norm(v).startswith("flock") for v in raw.iloc[i]
        ):
            return i
    raise ValueError("Header row not found")

def load_sheet(blob: bytes) -> pd.DataFrame:
    raw = pd.read_excel(BytesIO(blob), header=None)
    hdr = _find_header(raw)
    df = raw.iloc[hdr + 1 :].copy()
    df.columns = raw.iloc[hdr].astype(str).str.strip()
    df = rename_cols(df)
    df = df.loc[:, ~df.columns.duplicated()]
    df.dropna(how="all", inplace=True)
    return df.reset_index(drop=True)

# ─── تحديد نوع الملف ──────────────────────────────────────────────
def role(df: pd.DataFrame) -> str:
    cols = set(df.columns)
    if "Qty_Received" in cols and "Qty_Delivered" not in cols:
        return "received"
    if "Qty_Delivered" in cols and "Qty_Received" not in cols:
        return "delivered"
    raise ValueError("Unable to determine file role")

# ─── دوال مطابقة القطيع والتجميع ─────────────────────────────────
def clean_flock(s: str) -> str:
    return str(s).strip().upper()

def _base(f: str) -> str:
    p = f.split("-")
    return "-".join(p[:-1]) if p[-1].isdigit() else f

def fmap(*dfs) -> Dict[str, str]:
    best = {}
    for df in dfs:
        for f in df["Flock"]:
            f = clean_flock(f)
            b = _base(f)
            if len(f) > len(best.get(b, "")):
                best[b] = f
    return best

def canon(df: pd.DataFrame, mp: Dict[str, str]) -> pd.DataFrame:
    return df.assign(
        Flock=lambda d: d["Flock"].apply(
            lambda f: mp.get(_base(clean_flock(f)), clean_flock(f))
        )
    )

def split_flock(s: str) -> Dict[str, str]:
    p = s.split("-")
    site = p[0]
    house = p[-1] if p[-1].isdigit() else "-"
    flock = "-".join(p[1:-1]) if house != "-" else "-".join(p[1:])
    return {"Site": site, "Flock": flock, "House": house}

def normalize(df: pd.DataFrame, qty: str) -> pd.DataFrame:
    df = df.copy()
    df[qty] = pd.to_numeric(df[qty], errors="coerce").fillna(0)
    df = df[df[qty] > 0]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.date
    df.dropna(subset=["Date"], inplace=True)
    return df

def aggregate(df: pd.DataFrame, qty: str, code_col: str) -> pd.DataFrame:
    return (
        df.groupby(["Flock", "Date"], as_index=False)
        .agg({qty: "sum", code_col: "first"})
    )

# ─── كشف التكرار ─────────────────────────────────────────────────
def find_duplicates(df: pd.DataFrame, qty_col: str) -> set[tuple]:
    dup = df[df.duplicated(subset=["Flock", "Date", qty_col], keep=False)]
    return set(zip(dup["Flock"], dup["Date"], dup[qty_col]))

# ─── نهاية الـ API ────────────────────────────────────────────────
@app.post("/analyze_feed", response_model=Resp)
async def analyze_feed(
    received_file: Optional[UploadFile] = File(None),
    delivered_file: Optional[UploadFile] = File(None),
    file1: Optional[UploadFile] = File(None),
    file2: Optional[UploadFile] = File(None),
):
    # 1. التأكد من وجود ملفين
    files = [f for f in (received_file, delivered_file, file1, file2) if f is not None]
    if len(files) != 2:
        raise HTTPException(422, "Exactly two Excel files are required.")

    # 2. تحميل البيانات
    try:
        dfA = load_sheet(await files[0].read())
        dfB = load_sheet(await files[1].read())
    except Exception as e:
        raise HTTPException(400, f"Excel parse error: {e}")

    # 3. تحديد أيهما Received وأيهما Delivered
    try:
        roleA, roleB = role(dfA), role(dfB)
    except ValueError as e:
        raise HTTPException(422, str(e))

    rec_df = dfA if roleA == "received" else dfB
    del_df = dfA if roleA == "delivered" else dfB

    # 4. تأمين وجود أعمدة الأكواد إذا غابت في بعض الملفات
    for df, col in ((rec_df, "Code_Received"), (del_df, "Code_Delivered")):
        if col not in df.columns:
            df[col] = None

    # 5. تنظيف وتوحيد
    rec_norm = normalize(rec_df[["Flock", "Date", "Qty_Received", "Code_Received"]],
                         "Qty_Received")
    del_norm = normalize(del_df[["Flock", "Date", "Qty_Delivered", "Code_Delivered"]],
                         "Qty_Delivered")

    mp = fmap(rec_norm, del_norm)
    rec_norm = canon(rec_norm, mp)
    del_norm = canon(del_norm, mp)

    # 6. التكرارات
    dup_rec = find_duplicates(rec_norm, "Qty_Received")
    dup_del = find_duplicates(del_norm, "Qty_Delivered")

    # 7. التجميع
    rec_ag = aggregate(rec_norm, "Qty_Received", "Code_Received")
    del_ag = aggregate(del_norm, "Qty_Delivered", "Code_Delivered")

    rec_idx = {(r.Flock, r.Date): r for _, r in rec_ag.iterrows()}
    del_idx = {(r.Flock, r.Date): r for _, r in del_ag.iterrows()}

    rec_rows, del_rows = [], []
    for key in rec_idx.keys() | del_idx.keys():
        r = rec_idx.get(key)
        d = del_idx.get(key)

        # صف الاستلام
        if r is not None:
            data_r = {
                **split_flock(r.Flock),
                "Date": str(r.Date),
                "Qty_Received": float(r.Qty_Received),
                "Code_Received": r.Code_Received or "",
                "Error Details": "",
            }
            has_err_r = (r.Flock, r.Date, r.Qty_Received) in dup_rec
            if has_err_r:
                data_r["Error Details"] = "Duplicate row"
            rec_rows.append(FeedRow(data=data_r, has_error=has_err_r))

        # صف التوصيل
        if d is not None:
            data_d = {
                **split_flock(d.Flock),
                "Date": str(d.Date),
                "Qty_Delivered": float(d.Qty_Delivered),
                "Code_Delivered": d.Code_Delivered or "",
                "Error Details": "",
            }
            has_err_d = (d.Flock, d.Date, d.Qty_Delivered) in dup_del
            if has_err_d:
                data_d["Error Details"] = "Duplicate row"
            del_rows.append(FeedRow(data=data_d, has_error=has_err_d))

        # مقارنة الاستلام والتوصيل
        if r is None:
            del_rows[-1].data["Error Details"] += "; No received record"
            del_rows[-1].has_error = True
        elif d is None:
            rec_rows[-1].data["Error Details"] += "; No delivered record"
            rec_rows[-1].has_error = True
        else:
            # فرق الكمية
            diff = abs(float(r.Qty_Received) - float(d.Qty_Delivered))
            if diff > TOL:
                msg = f"Mismatch {r.Qty_Received:.2f} vs {d.Qty_Delivered:.2f}"
                for row in (rec_rows[-1], del_rows[-1]):
                    row.data["Error Details"] += f"; {msg}"
                    row.has_error = True

            # اختلاف كود العلف
            if (r.Code_Received or d.Code_Delivered) and (r.Code_Received != d.Code_Delivered):
                for row in (rec_rows[-1], del_rows[-1]):
                    row.data["Error Details"] += "; Formula mismatch"
                    row.has_error = True

    return Resp(received_data=rec_rows, delivered_data=del_rows)

# ─── تشغيل الخادم محليًا ──────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

