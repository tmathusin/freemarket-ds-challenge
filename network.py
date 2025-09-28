import re, math, warnings
import numpy as np
import pandas as pd
import networkx as nx



def build_client_network(clients: pd.DataFrame,
                         accounts: pd.DataFrame,
                         transfers: pd.DataFrame):
    import networkx as nx
    import re, math, warnings
    
    def quantile_label(series: pd.Series, q_hi=0.9, q_mid=0.6):
        if series.empty:
            return pd.Series(index=series.index, dtype="object")
        hi = series.quantile(q_hi)
        mid = series.quantile(q_mid)
        return series.apply(lambda v: "High" if v >= hi else ("Medium" if v >= mid else "Low"))
    
    """
    Build a directed client-to-client graph from transfers and compute node metrics.
    Returns: nodes_df, edges_df, G
    """

    # Map account -> client
    acc_map = (
        accounts[["account_id", "hub_spot_deal_id"]]
        .dropna()
        .drop_duplicates()
    )
    acc_map["hub_spot_deal_id"] = pd.to_numeric(acc_map["hub_spot_deal_id"], errors="coerce").astype("Int64")
    acc2client = dict(zip(acc_map["account_id"].astype(int), acc_map["hub_spot_deal_id"]))

    # Prepare transfer edges at client level
    t = transfers.dropna(subset=["sender_account_id", "recipient_account_id"]).copy()
    t["sender_account_id"]   = t["sender_account_id"].astype(int)
    t["recipient_account_id"] = t["recipient_account_id"].astype(int)
    t["sender_client"]       = t["sender_account_id"].map(acc2client)
    t["recipient_client"]    = t["recipient_account_id"].map(acc2client)
    t = t.dropna(subset=["sender_client", "recipient_client"])

    # Aggregate edge weights
    value_col = "normalised_amount" if "normalised_amount" in t.columns else "NormalisedAmount"
    id_col    = "transfer_id" if "transfer_id" in t.columns else ("TransferId" if "TransferId" in t.columns else None)

    edges_df = (
        t.groupby(["sender_client", "recipient_client"], as_index=False)
         .agg(edge_amount=(value_col, "sum"),
              edge_count=(id_col, "count") if id_col else (value_col, "size"))
    )

    # Build directed graph
    G = nx.DiGraph()
    for _, r in edges_df.iterrows():
        u, v = int(r["sender_client"]), int(r["recipient_client"])
        w, c = float(r["edge_amount"]), int(r["edge_count"])
        if G.has_edge(u, v):
            G[u][v]["weight"] += w
            G[u][v]["count"]  += c
        else:
            G.add_edge(u, v, weight=w, count=c)

    # Ensure all clients appear as nodes (even if isolated)
    for cid in clients["hub_spot_deal_id"].dropna().astype(int).unique():
        G.add_node(int(cid))

    # Node metrics
    in_strength  = {n: 0.0 for n in G.nodes()}
    out_strength = {n: 0.0 for n in G.nodes()}
    for u, v, d in G.edges(data=True):
        out_strength[u] += d.get("weight", 0.0)
        in_strength[v]  += d.get("weight", 0.0)

    in_degree  = dict(G.in_degree())
    out_degree = dict(G.out_degree())

    ug = G.to_undirected()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        betweenness = (
            nx.betweenness_centrality(ug, normalized=True, k=None)
            if ug.number_of_nodes() <= 4000
            else nx.betweenness_centrality(ug, normalized=True, k=400)
        )

    # Assemble nodes_df
    nodes_df = pd.DataFrame({"hub_spot_deal_id": list(G.nodes())})
    nodes_df["in_degree"]    = nodes_df["hub_spot_deal_id"].map(in_degree).fillna(0).astype(int)
    nodes_df["out_degree"]   = nodes_df["hub_spot_deal_id"].map(out_degree).fillna(0).astype(int)
    nodes_df["in_strength"]  = nodes_df["hub_spot_deal_id"].map(in_strength).fillna(0.0)
    nodes_df["out_strength"] = nodes_df["hub_spot_deal_id"].map(out_strength).fillna(0.0)
    nodes_df["betweenness"]  = nodes_df["hub_spot_deal_id"].map(betweenness).fillna(0.0)

    # Join metadata (company_name etc.)
    meta_cols = [c for c in ["company_name","group_name","vertical","segment","industry","state",
                             "risk_rating","pod","group_country_incorp","company_country_incorp"]
                 if c in clients.columns]
    nodes_df = nodes_df.merge(
        clients[["hub_spot_deal_id"] + meta_cols].drop_duplicates(),
        on="hub_spot_deal_id", how="left"
    )

    # Role labelling
    deg_label = quantile_label(nodes_df["in_degree"] + nodes_df["out_degree"])
    str_label = quantile_label(nodes_df["in_strength"] + nodes_df["out_strength"])
    bet_label = quantile_label(nodes_df["betweenness"])

    def role_row(i):
        d, s, b = deg_label.iloc[i], str_label.iloc[i], bet_label.iloc[i]
        if d == "High" and s == "High":
            return "Hub"
        if b == "High":
            return "Bridge"
        if d == "Medium" or s == "Medium":
            return "Connector"
        return "Peripheral"

    nodes_df["network_role"] = [role_row(i) for i in range(len(nodes_df))]

    return nodes_df, edges_df, G

# def build_client_network(clients: pd.DataFrame,
#                          accounts: pd.DataFrame,
#                          transfers: pd.DataFrame):
#     # Map account_id -> ClientId
#     acc_map = accounts[["account_id", "hub_spot_deal_id"]].dropna().drop_duplicates()
#     acc_map["hub_spot_deal_id"] = pd.to_numeric(acc_map["hub_spot_deal_id"], errors="coerce").astype("Int64")
#     acc2client = dict(zip(acc_map["account_id"].astype(int), acc_map["hub_spot_deal_id"]))

#     # Prepare edges at client level
#     t = transfers.dropna(subset=["sender_account_id", "recipient_account_id"]).copy()
#     t["sender_account_id"] = t["sender_account_id"].astype(int)
#     t["recipient_account_id"] = t["recipient_account_id"].astype(int)
#     t["sender_client"] = t["sender_account_id"].map(acc2client)
#     t["recipient_client"] = t["recipient_account_id"].map(acc2client)
#     t = t.dropna(subset=["sender_client", "recipient_client"])

#     # Aggregate weights
#     agg = (t.groupby(["sender_client", "recipient_client"])
#              .agg(edge_amount=("normalised_amount", "sum"),
#                   edge_count=("transfer_id", "count"))
#              .reset_index())

#     # Build directed graph G
#     import networkx as nx
#     G = nx.DiGraph()
#     for _, r in agg.iterrows():
#         u, v = int(r["sender_client"]), int(r["recipient_client"])
#         w, c = float(r["edge_amount"]), int(r["edge_count"])
#         if G.has_edge(u, v):
#             G[u][v]["weight"] += w
#             G[u][v]["count"] += c
#         else:
#             G.add_edge(u, v, weight=w, count=c)

#     # Add isolated clients as nodes
#     for cid in clients["hub_spot_deal_id"].dropna().astype(int).unique():
#         G.add_node(int(cid))

#     # Compute node metrics -> nodes_df (in/out degree & strength, betweenness, metadata, role)
#     # ... (metrics code follows in your notebook) ...
#     return nodes, agg, G

def build_flow_pairs(transfers):
    """
    Aggregate transfers into directional 'flow pairs' between participants.

    Output columns:
      - source_id
      - destination_id
      - transfer_count
      - total_value
    """

    # column names 
    source_col = 'sender_account_id'
    dest_col = 'recipient_account_id'
    ref_col = 'transfer_id'
    amount_col = 'normalised_amount'
    
    # aggregate

    agg = (
        transfers
        .groupby([source_col, dest_col], dropna=False)
        .agg(
            transfer_count=(ref_col, 'count'),
            total_value=(amount_col, 'sum')
        )
        .reset_index()
        .rename(columns={source_col: 'source_id', dest_col: 'destination_id'})
    )

    return agg


def participant_metrics(flow_pairs):
    """
    Compute per-participant stats from directional flow pairs.

    Output columns:
      - participant_id
      - unique_destinations   (distinct counterparties they send to)
      - unique_sources        (distinct counterparties they receive from)
      - unique_counterparties (sum of the above)
      - total_sent            (sum of total_value where they are source)
      - total_received        (sum of total_value where they are destination)
      - has_two_way_flow      (True if flows both ways)
      - interaction_profile   ('Hub'/'Broker'/'Spoke'/'Member'/'Isolated')
    """
    required = {'source_id', 'destination_id', 'transfer_count', 'total_value'}
    missing = required.difference(flow_pairs.columns)
    if missing:
        raise ValueError(f"'flow_pairs' missing columns: {sorted(missing)}")

    fp = flow_pairs.copy()

    # Unique counterpart counts
    unique_dest = fp.groupby('source_id')['destination_id'].nunique().rename('unique_destinations')
    unique_src  = fp.groupby('destination_id')['source_id'].nunique().rename('unique_sources')

    # Totals
    sent = fp.groupby('source_id')['total_value'].sum().rename('total_sent')
    received = fp.groupby('destination_id')['total_value'].sum().rename('total_received')

    participants = pd.concat([unique_dest, unique_src, sent, received], axis=1).fillna(0.0)
    participants.index.name = 'participant_id'
    participants['unique_counterparties'] = participants['unique_destinations'] + participants['unique_sources']

    # Two-way flow without temp columns
    corridors = set(tuple(x) for x in fp[['source_id','destination_id']].itertuples(index=False, name=None))
    two_way = set()
    for s, d in corridors:
        if (d, s) in corridors:
            two_way.add(s); two_way.add(d)
    participants['has_two_way_flow'] = participants.index.map(lambda x: x in two_way)

    # profile rules
    def _profile(row):
        if row['unique_counterparties'] == 0:
            return 'Isolated'
        if row['unique_destinations'] >= 5 and row['unique_sources'] >= 5:
            return 'Hub'
        if (row['unique_destinations'] >= 5 and row['unique_sources'] < 5) or \
           (row['unique_sources'] >= 5 and row['unique_destinations'] < 5):
            return 'Broker'
        if row['unique_counterparties'] <= 2:
            return 'Peripheral Member'
        return 'Regular Member'

    participants['interaction_profile'] = participants.apply(_profile, axis=1)
    return participants.reset_index()


def top_participants(participants, n = 15):
    """
    Rank participants by size and value of activity.
    """
    order = ['unique_counterparties', 'total_sent', 'total_received']
    keep = [c for c in order if c in participants.columns]
    return participants.sort_values(keep, ascending=False).head(n)

def build_counterparty_metrics(df, entity_col, amount_col, role):
    """
    Aggregates to one row per counterparty with total £ value, tx count, and avg ticket.
    - entity_col: the *standardised* name column
    - amount_col: numeric amount column (use your 'normalised_amount')
    - role: 'remitter' or 'beneficiary'
    """
    tmp = df[[entity_col, amount_col]].copy()

    # clean up
    tmp[entity_col] = (
        tmp[entity_col]
        .fillna("Unknown")
        .astype(str)
        .str.strip()
        .replace({"": "Unknown"})
    )
    tmp[amount_col] = pd.to_numeric(tmp[amount_col], errors="coerce").fillna(0)

    g = (
        tmp.groupby(entity_col, dropna=False)
           .agg(value_total=(amount_col, "sum"),
                volume_total=(amount_col, "size"))
           .reset_index()
           .rename(columns={entity_col: "counterparty"})
    )
    g["role"] = role
    return g

def classify_quadrants(metrics):
    df = metrics.copy()
    df["_value_m"] = df["value_total"] / 1e6
    df["_volume"]  = df["volume_total"]
    v_med = float(df["_value_m"].median())
    q_med = float(df["_volume"].median())

    def _quad(row):
        if row["_value_m"] >= v_med and row["_volume"] >= q_med:
            return "High Value / High Volume"
        if row["_value_m"] < v_med and row["_volume"] >= q_med:
            return "Low Value / High Volume"
        if row["_value_m"] >= v_med and row["_volume"] < q_med:
            return "High Value / Low Volume"
        return "Low Value / Low Volume"

    out = df.copy()
    out["quadrant"] = out.apply(_quad, axis=1)
    cols = ["counterparty", "role", "value_total", "volume_total", "quadrant"]
    return out[cols].sort_values(by=["value_total"], ascending=False)
