from pathlib import Path
import pandas as pd
from openpyxl import load_workbook


# =========================
# CONFIG
# =========================
FOLDER_PATH = r"/path/to/your/folder"   # <- change this
OUTPUT_FILE = r"/path/to/output/impact_synthesis_summary.xlsx"  # <- change this

SHEET_NAME = "Impact Synthesis"
DATE_CELL = "B2"

# What to search in column A (case-insensitive "contains")
TARGETS = {
    "income": "Income",
    "nav_adjusting_entries": "Nav Adjusting Entries",
    "total_tna_var_total_perf": "TOTAL TNA VAR / TOTAL PERF"
}


def find_value_in_col_a(ws, text_to_find, value_col=12):
    """
    Looks in column A for a cell containing `text_to_find` (case-insensitive).
    If found, returns value from `value_col` on the same row (default col L = 12).
    If not found, returns 0.
    """
    search_text = str(text_to_find).strip().lower()

    for row in ws.iter_rows(min_col=1, max_col=1):  # only column A
        cell = row[0]
        cell_value = cell.value

        if cell_value is None:
            continue

        if search_text in str(cell_value).strip().lower():
            val = ws.cell(row=cell.row, column=value_col).value
            return 0 if val is None else val

    return 0


def process_file(file_path):
    """
    Reads one Excel file and extracts:
    - date from B2
    - income (col L on row where col A contains 'Income')
    - nav_adjusting_entries
    - total_tna_var_total_perf
    """
    try:
        wb = load_workbook(file_path, data_only=True)
    except Exception as e:
        print(f"❌ Could not open {file_path.name}: {e}")
        return None

    if SHEET_NAME not in wb.sheetnames:
        print(f"⚠️ Sheet '{SHEET_NAME}' not found in {file_path.name}")
        return None

    ws = wb[SHEET_NAME]

    # Date in B2
    date_value = ws[DATE_CELL].value
    if date_value is None:
        print(f"⚠️ No date in {DATE_CELL} in {file_path.name}")
        return None

    # Extract values
    income_val = find_value_in_col_a(ws, TARGETS["income"], value_col=12)  # L
    nav_adj_val = find_value_in_col_a(ws, TARGETS["nav_adjusting_entries"], value_col=12)
    total_perf_val = find_value_in_col_a(ws, TARGETS["total_tna_var_total_perf"], value_col=12)

    return {
        "date": date_value,
        "income": income_val,
        "nav_adjusting_entries": nav_adj_val,
        "total_tna_var_total_perf": total_perf_val,
        "source_file": file_path.name,  # optional, useful for debugging
    }


def main():
    folder = Path(FOLDER_PATH)

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    # Excel extensions to scan
    excel_files = []
    for ext in ("*.xlsx", "*.xlsm", "*.xltx", "*.xltm"):
        excel_files.extend(folder.glob(ext))

    # Exclude temporary Excel files like ~$file.xlsx
    excel_files = [f for f in excel_files if not f.name.startswith("~$")]

    if not excel_files:
        print("No Excel files found.")
        return

    rows = []
    for file_path in excel_files:
        result = process_file(file_path)
        if result is not None:
            rows.append(result)

    if not rows:
        print("No valid data extracted.")
        return

    df = pd.DataFrame(rows)

    # Convert date safely + sort ascending
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    # Optional: remove source_file column if you don't want it in final output
    # df = df[["date", "income", "nav_adjusting_entries", "total_tna_var_total_perf"]]

    # Write to Excel
    df.to_excel(OUTPUT_FILE, index=False)

    print(f"✅ Done. Output saved to: {OUTPUT_FILE}")
    print(df)


if __name__ == "__main__":
    main()