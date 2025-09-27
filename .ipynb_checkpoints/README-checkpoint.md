# Freemarket DS Interview — Single Notebook + Python Modules


## Project Structure
```
Code Test/
├─ fmfx_solution.ipynb
├─ requirements.txt
├─ script/
│  ├─ __init__.py
│  ├─ data_io.py
│  ├─ cleaning.py
│  ├─ network.py
│  └─ viz.py
└─ data/
   └─ Test for Data Science role.xlsx
```

## Setup & Run (Windows PowerShell)

1) Open PowerShell in the project folder:
```powershell
cd "Code Test"
```

2) (Optional) Create & activate a virtual environment:
```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

3) Install dependencies:
- **Using requirements.txt (recommended)**
  ```powershell
  pip install -r requirements.txt
  ```
- **Or install individually**
  ```powershell
  pip install jupyter pandas matplotlib
  pip install networkx
  ```

4) Launch Jupyter and open the notebook:
```powershell
jupyter notebook fmfx_solution.ipynb
```
Then run the notebook top-to-bottom. It reads the Excel from `data/Test for Data Science role.xlsx`.

## What the Notebook Does
- **Load & Normalise Data**: `script.data_io.load_all_tables` reads **Client, Accounts, Deposits, Withdrawals, Transfers** (headers are on row 2 of the workbook), drops empty columns, and converts date-like fields.
- **Transfer Network**: `script.network.build_edge_table` and `node_metrics` generate:
  - Edge list (sender → receiver) with transaction counts and total amounts.
  - Per-client metrics: in/out degree, degree, total sent/received, reciprocity participation.
  - Simple role classification: `Hub / Broker / Spoke / Member`.
- **Outside-Network Flows**: `script.cleaning.add_canonical_entities` standardises **Remitter** (Deposits) and **Beneficiary** (Withdrawals) names using conservative fuzzy grouping, then `aggregate_flows` sizes true flows.
- **Visuals**: `script.viz.bar_top_series` draws quick bar charts. `try_plot_network` draws a light network if `networkx` is installed (otherwise it logs and skips).

## Dependencies (`requirements.txt`)
```
jupyter
pandas
matplotlib
networkx
```

## Notes & Assumptions
- Headers in the provided workbook are on the **second row** (index 1). The loaders are configured accordingly.
- Fuzzy grouping uses normalisation + `difflib.SequenceMatcher` with a high threshold (0.9) to avoid false merges. For production, consider `rapidfuzz` and add blocking by country/IBAN/SWIFT.
- Role thresholds are intentionally simple and can be tuned based on data size and business context.

## Troubleshooting
- **Module import errors**: Ensure you run the notebook from the project root so `Code Test` is on `sys.path` (the first cell handles this).
- **Missing packages**: Re-run `pip install` commands inside the active virtual environment.
- **Plots not showing**: Make sure you call `plt.show()` (the notebook includes this).
