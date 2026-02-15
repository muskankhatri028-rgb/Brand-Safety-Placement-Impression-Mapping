# Brand-Safety-Placement-Impression-Mapping
This Python script compares impressions from multiple DV360 CSV reports with a summary Excel file (Parul’s report). It extracts Date, Advertiser, and Impressions, normalizes advertiser names, merges both datasets, and generates an output with DV360 impressions, Parul impressions, Delta, and Delta%.
# CODE 
import pandas as pd
import os
import re
from glob import glob

# ================= PATHS =================
CSV_FOLDER_PATH = r"C:\Users\LP-353\Desktop\muskan\mfilterit\ITC\from jan 2025\brand safety Youtube -2025\BS code test impressions"
EXCEL_PATH = r"C:\Users\LP-353\Desktop\muskan\mfilterit\ITC\from jan 2025\brand safety Youtube -2025\summary from parul\Dec'25\ITC_Summary_Dec v.04 till 17th.xlsx"
EXCEL_SHEET = "_select_inserted_date_review_st"
OUTPUT_PATH = r"C:\Users\LP-353\Desktop\muskan\mfilterit\ITC\from jan 2025\brand safety Youtube -2025\BS code test impressions\final output delta file.xlsx"

# ================= HELPERS =================
def normalize(text):
    return re.sub(r'[^a-z0-9]', '', str(text).lower())

def safe_first(series):
    series = series.dropna()
    return series.iloc[0] if not series.empty else None

# ================= CSV PROCESSING =================
csv_rows = []

csv_files = glob(os.path.join(CSV_FOLDER_PATH, "*.csv"))

for file in csv_files:
    try:
        df = pd.read_csv(file, low_memory=False)
        df.columns = df.columns.str.strip()

        # ---------- Advertiser ----------
        if "Advertiser" not in df.columns:
            print(f"⚠ Skipped (Advertiser missing): {os.path.basename(file)}")
            continue

        advertiser = safe_first(df["Advertiser"])
        if advertiser is None:
            print(f"⚠ Skipped (Advertiser empty): {os.path.basename(file)}")
            continue

        # ---------- Date (ROBUST FIX) ----------
        if "Date" not in df.columns:
            print(f"⚠ Skipped (Date column missing): {os.path.basename(file)}")
            continue

        date_series = pd.to_datetime(df["Date"], errors="coerce").dropna()

        if date_series.empty:
            print(f"⚠ Skipped (No valid date found): {os.path.basename(file)}")
            continue

        date_val = date_series.iloc[0].date()

        # ---------- Impressions ----------
        if "Impressions" not in df.columns:
            print(f"⚠ Skipped (Impressions missing): {os.path.basename(file)}")
            continue

        impressions_series = pd.to_numeric(
            df["Impressions"], errors="coerce"
        ).dropna()

        if impressions_series.empty:
            print(f"⚠ Skipped (Impressions empty): {os.path.basename(file)}")
            continue

        impressions = int(impressions_series.iloc[-1])

        csv_rows.append({
            "date": date_val,
            "advertiser": advertiser,
            "norm_adv": normalize(advertiser),
            "Dv360 file": impressions
        })

    except Exception as e:
        print(f"❌ Error reading {os.path.basename(file)} → {e}")

csv_df = pd.DataFrame(csv_rows)

# ================= EXCEL PROCESSING =================
excel_df = pd.read_excel(EXCEL_PATH, sheet_name=EXCEL_SHEET)
excel_df.columns = excel_df.columns.str.strip().str.lower()

excel_df["date"] = pd.to_datetime(
    excel_df["inserted_date"], errors="coerce"
).dt.date

excel_df["advertiser"] = excel_df["campaign_name"].astype(str)
excel_df["norm_adv"] = excel_df["advertiser"].apply(normalize)

excel_df["sum(impressions)"] = pd.to_numeric(
    excel_df["sum(impressions)"], errors="coerce"
).fillna(0)

excel_df = (
    excel_df
    .groupby(["date", "advertiser", "norm_adv"], as_index=False)
    .agg(**{"parul file": ("sum(impressions)", "sum")})
)

# ================= MERGE =================
final_df = pd.merge(
    csv_df,
    excel_df,
    on=["date", "norm_adv"],
    how="left"
)

final_df["parul file"] = final_df["parul file"].fillna(0)

# ================= CALCULATIONS =================
final_df["Delta"] = final_df["Dv360 file"] - final_df["parul file"]

final_df["Delta%"] = (
    (final_df["Delta"] / final_df["Dv360 file"])
    .replace([float("inf"), -float("inf")], 0)
    .fillna(0)
    * 100
).round(2)

# ================= FINAL FORMAT =================
final_df = final_df[[
    "date",
    "advertiser_x",
    "Dv360 file",
    "parul file",
    "Delta",
    "Delta%"
]]

final_df.columns = [
    "Date",
    "Advertiser",
    "Dv360 file",
    "parul file",
    "Delta",
    "Delta%"
]

final_df.sort_values(["Date", "Advertiser"], inplace=True)

# ================= EXPORT =================
final_df.to_excel(OUTPUT_PATH, index=False)

print("✅ Final output generated successfully")
print(f"📁 Output saved at: {OUTPUT_PATH}")
