
import pandas as pd

def _read_sheet(excel_path: str, sheet_name: str):
    """
    All sheets in the provided workbook have their headers in the 2nd row (index=1).
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=1)
    # Drop columns that are entirely NaN
    df = df.dropna(axis=1, how="all")
    return df

def load_all_tables(excel_path: str):
    """
    Returns a dict with keys: client, accounts, deposits, withdrawals, transfers
    """
    sheets = ["Client", "Accounts", "Deposits", "Withdrawals", "Transfers"]
    dfs = {s.lower(): _read_sheet(excel_path, s) for s in sheets}
    # Normalise column names
    for k, df in dfs.items():
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
        )
        dfs[k] = df
    return dfs
