import pandas as pd
from openpyxl import load_workbook


excel_path = r"C:/Users/h24826/BNP Paribas/GFI - GCC - Structured Credit - Documents/ABS_BDD/05_BASE_ESG/ABS-RMBS-BASE-v6.1.xlsm"

funds = [
    "FLABSA_AL",
    "FLABSI_AL",
    "FLABSO_AL",
    "H21892EU_AL",
    "VALFCPABS_AL"
]


def clean_ticker(x):

    if pd.isna(x):
        return None

    x = str(x)

    if "_" in x:
        x = x.split("_")[0]

    return x.strip()


def build_esg_db(excel_path):

    esg_db = pd.read_excel(
        excel_path,
        sheet_name="Coverage",
        usecols="A:F",
        nrows=1000,
        engine="openpyxl"
    )

    esg_db = esg_db[
        [
            "Tickers",
            "CUSIP",
            "New ESG"
        ]
    ]

    esg_db.columns = [
        "Ticker",
        "Issuer ID",
        "New ESG"
    ]

    esg_db["Ticker"] = esg_db["Ticker"].apply(
        clean_ticker
    )

    esg_db = esg_db.drop_duplicates(
        subset="Ticker"
    )

    return esg_db


def load_fund(fund_ws):

    df = pd.read_excel(
        excel_path,
        sheet_name=fund_ws,
        header=7,
        usecols="A,B,C,K",
        engine="openpyxl"
    )

    df.columns = [
        "Ticker",
        "ISIN",
        "CUSIP",
        "Issuer ID_fm"
    ]

    df["Ticker"] = df["Ticker"].apply(
        clean_ticker
    )

    return df


def issuer_check(
        fund_ws,
        esg_db
):

    fund_df = load_fund(
        fund_ws
    )

    merged = fund_df.merge(
        esg_db,
        on="Ticker",
        how="left"
    )

    output = []

    for _, row in merged.iterrows():

        issuer_fm = row[
            "Issuer ID_fm"
        ]

        issuer_esg = row[
            "Issuer ID"
        ]

        score_esg = row[
            "New ESG"
        ]

        if pd.isna(
            score_esg
        ):
            continue

        if (
            pd.isna(
                issuer_fm
            )
            and
            not pd.isna(
                issuer_esg
            )
        ):

            output.append(
                {
                    "ISIN":
                    row["ISIN"],

                    "CUSIP":
                    row["CUSIP"],

                    "Expected ESG Issuer ID":
                    issuer_esg,

                    "Comments":
                    "missing"
                }
            )

        elif (
            str(
                issuer_fm
            )
            !=
            str(
                issuer_esg
            )
        ):

            output.append(
                {
                    "ISIN":
                    row["ISIN"],

                    "CUSIP":
                    row["CUSIP"],

                    "Expected ESG Issuer ID":
                    issuer_esg,

                    "Comments":
                    "miss-linked"
                }
            )

    return pd.DataFrame(
        output
    )


def export_results(
        results
):

    wb = load_workbook(
        excel_path,
        keep_vba=True
    )

    if (
        "ESG_Challenge"
        in
        wb.sheetnames
    ):

        ws = wb[
            "ESG_Challenge"
        ]

        ws.delete_rows(
            1,
            ws.max_row
        )

    else:

        ws = wb.create_sheet(
            "ESG_Challenge"
        )

    row_excel = 1

    for fund, df in results.items():

        ws.cell(
            row_excel,
            1,
            fund
        )

        row_excel += 1

        headers = [
            "ISIN",
            "CUSIP",
            "Expected ESG Issuer ID",
            "Comments"
        ]

        for i, h in enumerate(
            headers,
            1
        ):

            ws.cell(
                row_excel,
                i,
                h
            )

        row_excel += 1

        for _,
            r in df.iterrows():

            ws.cell(
                row_excel,
                1,
                r["ISIN"]
            )

            ws.cell(
                row_excel,
                2,
                r["CUSIP"]
            )

            ws.cell(
                row_excel,
                3,
                r[
                    "Expected ESG Issuer ID"
                ]
            )

            ws.cell(
                row_excel,
                4,
                r[
                    "Comments"
                ]
            )

            row_excel += 1

        row_excel += 2

    wb.save(
        excel_path
    )


def main():

    esg_db = build_esg_db(
        excel_path
    )

    results = {}

    for fund in funds:

        print(
            f"Checking {fund}"
        )

        results[
            fund
        ] = issuer_check(
            fund,
            esg_db
        )

    export_results(
        results
    )

    print(
        "Done"
    )


if __name__ == "__main__":

    main()