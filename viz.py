import pandas as pd
import matplotlib.pyplot as plt

def bar_top_series(df: pd.DataFrame, label_col: str, value_col: str, top_n=15, title=""):
    top = df.head(top_n)
    plt.figure()
    top.plot(kind="bar", x=label_col, y=value_col, legend=False, rot=45)
    plt.title(title or f"Top {top_n} by {value_col}")
    plt.tight_layout()

def plot_network(flow_pairs: pd.DataFrame, title = "Directional flow of traffic"):
    """
    Requires networkx to plot traffic in the network
    """
    import networkx as nx

    G = nx.DiGraph()
    for _, row in flow_pairs.iterrows():
        G.add_edge(str(row['source_id']), str(row['destination_id']),
                   weight=row.get('total_value', 1.0))

    plt.figure()
    pos = nx.spring_layout(G, seed=42, k=0.3)
    nx.draw_networkx_nodes(G, pos, node_size=50)

    # Draw arrows to indicate direction
    nx.draw_networkx_edges(
        G, pos,
        arrows=True,
        arrowsize=10,
        width=0.5,
        alpha=0.5,
        connectionstyle="arc3,rad=0.08"   # slight curve to show two-way traffic
    )

    plt.title(title)
    plt.axis('off')
    plt.tight_layout()
