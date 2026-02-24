from pathlib import Path
import pandas as pd
from openpyxl import load_workbook

# =========================
# CONFIG
# =========================
FOLDER_PATH = r"/path/to/your/folder"  # <-- change
OUTPUT_FILE = r"/path/to/output/impact_synthesis_summary.xlsx"  # <-- change

SHEET_NAME = "Impact Synthesis"
DATE_CELL = "B2"

TARGETS = {
    "income": "Income",
    "nav_adjusting_entries": "Nav Adjusting Entries",
    "total_tna_var_total_perf": "TOTAL TNA VAR / TOTAL PERF",
}

EXCEL_GLOBS = ("*.xlsx", "*.xlsm", "*.xltx", "*.xltm")


def find_value_in_col_a(ws, text_to_find, value_col=12):
    search_text = str(text_to_find).strip().lower()

    for (cell,) in ws.iter_rows(min_col=1, max_col=1):  # column A only
        v = cell.value
        if v is None:
            continue
        if search_text in str(v).strip().lower():
            val = ws.cell(row=cell.row, column=value_col).value  # col L = 12
            return 0 if val is None else val

    return 0


def try_open_workbook(file_path):
    """
    Try to open with openpyxl. If some files have invalid XML/styles,
    we catch and return (None, error_message).
    """
    try:
        wb = load_workbook(file_path, data_only=True, read_only=True)
        return wb, None
    except Exception as e:
        return None, str(e)


def process_file(file_path):
    wb, err = try_open_workbook(file_path)
    if wb is None:
        return None, err

    if SHEET_NAME not in wb.sheetnames:
        return None, f"Missing sheet '{SHEET_NAME}'"

    ws = wb[SHEET_NAME]

    date_value = ws[DATE_CELL].value
    if date_value is None:
        return None, f"Missing date in {SHEET_NAME}!{DATE_CELL}"

    income_val = find_value_in_col_a(ws, TARGETS["income"], value_col=12)
    nav_adj_val = find_value_in_col_a(ws, TARGETS["nav_adjusting_entries"], value_col=12)
    total_perf_val = find_value_in_col_a(ws, TARGETS["total_tna_var_total_perf"], value_col=12)

    return {
        "date": date_value,
        "income": income_val,
        "nav_adjusting_entries": nav_adj_val,
        "total_tna_var_total_perf": total_perf_val,
        "source_file": file_path.name,
    }, None


def main():
    folder = Path(FOLDER_PATH)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = []
    for g in EXCEL_GLOBS:
        files.extend(folder.glob(g))
    files = [f for f in files if not f.name.startswith("~$")]

    rows = []
    failures = []

    for f in files:
        result, err = process_file(f)
        if err is not None:
            failures.append({"file": f.name, "error": err})
            continue
        rows.append(result)

    if not rows:
        print("No valid data extracted. Check failures sheet/log.")
        df_fail = pd.DataFrame(failures)
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
            df_fail.to_excel(writer, sheet_name="failures", index=False)
        print(f"Saved failures log to: {OUTPUT_FILE}")
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    df_fail = pd.DataFrame(failures)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="data", index=False)
        if not df_fail.empty:
            df_fail.to_excel(writer, sheet_name="failures", index=False)

    print(f"✅ Done. Output saved to: {OUTPUT_FILE}")
    print(f"Read OK: {len(df)} files | Failed: {len(df_fail)} files")


if __name__ == "__main__":
    main()