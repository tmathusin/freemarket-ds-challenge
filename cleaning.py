import re
from difflib import SequenceMatcher
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
    # Title-casing for display purposes
    return " ".join(w.capitalize() for w in s.split())

def standardise_counterparty_names(df, col, threshold = 0.9):
    """
    Groups near-duplicates by simple pairwise SequenceMatcher on the normalized text.
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
    Aggregates totals per entity.
    Returns two columns: 'entity' and 'amount'.
    """

    out = (
        df.groupby(entity_col, as_index=False)[amount_col]
           .sum()
           .rename(columns={entity_col: "entity", amount_col: "amount"})
           .sort_values("amount", ascending=False)
           .reset_index(drop=True)
    )
    return out
