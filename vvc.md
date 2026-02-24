from pathlib import Path
import sys
import pandas as pd
from openpyxl import load_workbook

SHEET_NAME = "Impact Synthesis"
DATE_CELL = "B2"

TARGETS = {
    "income": "Income",
    "nav_adjusting_entries": "Nav Adjusting Entries",
    "total_tna_var_total_perf": "TOTAL TNA VAR / TOTAL PERF",
}


def find_value_in_col_a(ws, text_to_find, value_col=12):
    search_text = str(text_to_find).strip().lower()

    for (cell,) in ws.iter_rows(min_col=1, max_col=1):  # column A only
        v = cell.value
        if v is None:
            continue
        if search_text in str(v).strip().lower():
            val = ws.cell(row=cell.row, column=value_col).value  # column L
            return 0 if val is None else val

    return 0


def process_file(file_path: Path):
    try:
        wb = load_workbook(file_path, data_only=True, read_only=True)
    except Exception as e:
        return None, f"OPEN_ERROR: {e}"

    if SHEET_NAME not in wb.sheetnames:
        return None, f"MISSING_SHEET: {SHEET_NAME}"

    ws = wb[SHEET_NAME]

    date_value = ws[DATE_CELL].value
    if date_value is None:
        return None, f"MISSING_DATE: {DATE_CELL}"

    row = {
        "date": date_value,
        "income": find_value_in_col_a(ws, TARGETS["income"], value_col=12),
        "nav_adjusting_entries": find_value_in_col_a(ws, TARGETS["nav_adjusting_entries"], value_col=12),
        "total_tna_var_total_perf": find_value_in_col_a(ws, TARGETS["total_tna_var_total_perf"], value_col=12),
        "source_file": file_path.name,
    }
    return row, None


def main():
    if len(sys.argv) < 3:
        print("Usage: python extract_impact.py <cleaned_folder> <output_file>")
        sys.exit(1)

    cleaned_folder = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    if not cleaned_folder.exists():
        raise FileNotFoundError(f"Folder not found: {cleaned_folder}")

    files = sorted(cleaned_folder.glob("*.xlsx"))
    files = [f for f in files if not f.name.startswith("~$")]

    print(f"[INFO] Cleaned files found: {len(files)}")

    rows = []
    failures = []

    for i, f in enumerate(files, start=1):
        res, err = process_file(f)
        if err:
            failures.append({"file": f.name, "error": err})
        else:
            rows.append(res)

        if i % 50 == 0:
            print(f"[INFO] {i}/{len(files)} processed | ok={len(rows)} | fail={len(failures)}")

    df = pd.DataFrame(rows)
    df_fail = pd.DataFrame(failures)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", ascending=True).reset_index(drop=True)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        if not df.empty:
            df.to_excel(writer, sheet_name="data", index=False)
        df_fail.to_excel(writer, sheet_name="failures", index=False)

    print(f"[DONE] Output written: {output_file}")
    print(f"[DONE] OK={len(df)} | FAIL={len(df_fail)}")


if __name__ == "__main__":
    main()