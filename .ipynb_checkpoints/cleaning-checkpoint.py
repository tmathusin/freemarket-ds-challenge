import re
from difflib import SequenceMatcher
import pandas as pd

import re
import pandas as pd

def norm_name(x):
    if not isinstance(x, str):
        return ""
    x = x.lower()
    x = re.sub(r"&", " and ", x)
    x = re.sub(r"[^a-z0-9]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def to_title(s):
    # Basic title-casing for display purposes
    return " ".join(w.capitalize() for w in s.split())

def standardise_counterparty_names(df, col, threshold = 0.9):
    """
    Groups near-duplicates by simple pairwise SequenceMatcher on the *normalized* text.
    """
    normalized_names = df[col].fillna("").astype(str).map(norm_name)

    # Build a simple canonical map using pairwise similarity (order-preserving)
    uniques = list(dict.fromkeys(normalized_names.tolist()))
    canon_map = {}
    assigned = set()

    for i, a in enumerate(uniques):
        if not a or a in assigned:
            continue
        canon = a
        canon_map[a] = canon
        for b in uniques[i+1:]:
            if not b or b in assigned:
                continue
            if SequenceMatcher(None, a, b).ratio() >= threshold:
                canon_map[b] = canon
                assigned.add(b)

    # Map to canonical (fallback to itself), then title-case for display
    std_series = normalized_names.map(lambda x: canon_map.get(x, x)).map(to_title)

    out = df.copy()
    out[f"{col}_standardised"] = std_series
    return out

def aggregate_flows(df, entity_col, amount_col):
    """
    Aggregates totals per entity only.
    Returns two columns: 'entity' and 'amount'.
    """
    # tmp = df[[entity_col, amount_col]].copy()
    # tmp[amount_col] = pd.to_numeric(tmp[amount_col], errors="coerce").fillna(0)

    out = (
        df.groupby(entity_col, as_index=False)[amount_col]
           .sum()
           .rename(columns={entity_col: "entity", amount_col: "amount"})
           .sort_values("amount", ascending=False)
           .reset_index(drop=True)
    )
    return out



# def _norm_name(x: str) -> str:
#     if not isinstance(x, str):
#         return ""
#     x = x.lower()
#     x = re.sub(r"&", " and ", x)
#     x = re.sub(r"[^a-z0-9]+", " ", x)
#     x = re.sub(r"\s+", " ", x).strip()
#     return x

# def _pick_col(cols, keywords):
#     for c in cols:
#         lc = c.lower()
#         if all(k in lc for k in keywords):
#             return c
#     return None

# def _pick_any(cols, variants):
#     for words in variants:
#         col = _pick_col(cols, words)
#         if col:
#             return col
#     return None

# def standardise_counterparty_names(df: pd.DataFrame, col: str | None = None, role: str | None = None, threshold: float = 0.9) -> pd.DataFrame:
#     cols = list(df.columns)
#     role_norm = (role or "").lower()

#     if col is None:
#         if role_norm in {"remitter", "deposit", "depositor"}:
#             col = _pick_any(cols, [
#                 ["deposit","remitter","name"],
#                 ["remitter","name"],
#                 ["sender","name"],
#                 ["payer","name"],
#             ])
#         elif role_norm in {"beneficiary","withdrawal","withdraw"}:
#             col = _pick_any(cols, [
#                 ["beneficiary","name"],
#                 ["recipient","name"],
#                 ["payee","name"],
#             ])
#         else:
#             col = _pick_any(cols, [["name"], ["counterparty","name"]])

#     if col is None or col not in df.columns:
#         raise KeyError(f"Could not find a suitable name column. Available: {cols}")

#     ser = df[col].fillna("").astype(str).map(_norm_name)
#     uniques = ser.unique().tolist()

#     blocks = {}
#     for u in uniques:
#         key = (u[:2] if len(u)>=2 else u) + f"_{len(u)//5}"
#         blocks.setdefault(key, []).append(u)

#     canon_map = {}
#     for _, items in blocks.items():
#         assigned = set()
#         for i, a in enumerate(items):
#             if a in assigned:
#                 continue
#             canon = a
#             canon_map[a] = canon
#             for b in items[i+1:]:
#                 if b in assigned:
#                     continue
#                 if SequenceMatcher(None, a, b).ratio() >= threshold:
#                     canon_map[b] = canon
#                     assigned.add(b)

#     out = df.copy()
#     out[f"{col}_standardised"] = ser.map(lambda x: canon_map.get(x, x))
#     out[f"{col}_canon"] = out[f"{col}_standardised"]  # back-compat
#     return out