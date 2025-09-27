import pandas as pd

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


def top_participants(participants: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """
    Rank participants by size and value of activity.
    """
    order = ['unique_counterparties', 'total_sent', 'total_received']
    keep = [c for c in order if c in participants.columns]
    return participants.sort_values(keep, ascending=False).head(n)
