import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx


def bar_top_series(df: pd.DataFrame, label_col: str, value_col: str, top_n=15, title=""):
    top = df.head(top_n)
    plt.figure()
    top.plot(kind="bar", x=label_col, y=value_col, legend=False, rot=45)
    plt.title(title or f"Top {top_n} by {value_col}")
    plt.tight_layout()


def plot_network_strength(
    G: nx.DiGraph,
    nodes_df: pd.DataFrame,
    out_path_base: str,
    max_nodes: int = 200,
    label_mode: str = "topk",         # "none" | "topk" | "auto" | "all"
    label_by: str = "strength",       # "strength" | "betweenness"
    topk_labels: int = 30,
    seed: int = 42,
):
    """
    Visualise client-to-client transfers:
      - node size ~ total strength (in+out normalised_amount)
      - node color ~ NetworkRole (Hub / Bridge / Connector / Peripheral)
      - edge width ~ transfer amount (log-scaled for readability)
      - labels = company_name (fallback -> client_id)
    
    Saves:
      - f"{out_path_base}_clean.png"
      - f"{out_path_base}_labeled.png"
    """
    if G.number_of_nodes() == 0:
        print("Graph is empty; skipping plot.")
        return

    # --- Score nodes for subgraph selection
    # strength = total in+out amount
    id_indexed = nodes_df.set_index("hub_spot_deal_id")
    node_strength = (id_indexed["in_strength"].fillna(0) + id_indexed["out_strength"].fillna(0)).to_dict()
    node_betw = id_indexed["betweenness"].fillna(0).to_dict()

    # degree + log(1+strength) to prioritise visible structure
    ranking_score = {
        n: (G.in_degree(n) + G.out_degree(n)) + (math.log1p(node_strength.get(n, 0)) if node_strength.get(n, 0) > 0 else 0.0)
        for n in G.nodes()
    }
    keep = [n for n, _ in sorted(ranking_score.items(), key=lambda kv: kv[1], reverse=True)[:max_nodes]]
    H = G.subgraph(keep).copy()

    # --- Node visuals
    strength_vec = np.array([node_strength.get(n, 0.0) for n in H.nodes()])
    size_vec = 120 + 24 * np.log1p(np.maximum(strength_vec, 0))  # px

    # Color by NetworkRole
    role_series = id_indexed["network_role"].reindex(list(H.nodes())).fillna("Peripheral")
    role_to_int = {"Hub":3, "Bridge":2, "Connector":1, "Peripheral":0}
    color_vec = role_series.map(role_to_int).fillna(0).to_numpy()

    # --- Edge visuals (width by amount, log-scaled)
    widths = []
    for u, v, d in H.edges(data=True):
        w = float(d.get("weight", 0.0))
        widths.append(0.4 + 1.6 * math.log1p(max(w, 0)))  # linewidth

    # --- Layout
    pos = nx.spring_layout(H, k=0.85 / math.sqrt(len(H.nodes()) + 1), seed=seed, weight="weight")

    # Helper: common draw routine
    def _draw(label_nodes=None, file_suffix="_clean"):
        plt.figure(figsize=(12, 9), dpi=180)
        nx.draw_networkx_edges(H, pos, alpha=0.20, width=widths, arrows=False)
        nodes = nx.draw_networkx_nodes(
            H, pos,
            node_size=size_vec,
            node_color=color_vec,
            cmap="viridis",
            linewidths=0.5,
            edgecolors="#333333",
            alpha=0.95,
        )

        # Legend for roles
        patches = [
            mpatches.Patch(color=plt.cm.viridis(role_to_int["Hub"]/3), label="Hub"),
            mpatches.Patch(color=plt.cm.viridis(role_to_int["Bridge"]/3), label="Bridge"),
            mpatches.Patch(color=plt.cm.viridis(role_to_int["Connector"]/3), label="Connector"),
            mpatches.Patch(color=plt.cm.viridis(role_to_int["Peripheral"]/3), label="Peripheral"),
        ]
        plt.legend(handles=patches, title="Network role", loc="lower left", frameon=False)

        # Labels (company_name -> fallback to id)
        if label_nodes:
            to_label = []
            for n in label_nodes:
                name = id_indexed.loc[n, "company_name"] if ("company_name" in id_indexed.columns and n in id_indexed.index) else None
                lbl = str(name) if (pd.notna(name) and str(name).strip()) else str(n)
                to_label.append((n, lbl))

            # Draw labels
            for n, lbl in to_label:
                x, y = pos[n]
                plt.text(x, y, lbl[:28], fontsize=7, ha="center", va="center", color="#111111",
                         bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.7))

        plt.axis("off")
        plt.tight_layout()
        outfile = f"{out_path_base}{file_suffix}.png"
        plt.savefig(outfile, dpi=220)
        plt.close()
        return outfile

    # --- Decide which nodes to label
    label_nodes = []
    if label_mode.lower() == "none":
        label_nodes = []
    elif label_mode.lower() == "all":
        label_nodes = list(H.nodes())
    else:
        if label_by == "betweenness":
            scores = node_betw
        else:
            scores = node_strength
        ranked = sorted(H.nodes(), key=lambda n: (scores.get(n, 0), ranking_score.get(n, 0)), reverse=True)
        if label_mode.lower() == "topk":
            label_nodes = ranked[:topk_labels]
        elif label_mode.lower() == "auto":
            # Heuristic: more labels if graph is small, fewer if large
            k = min(topk_labels, max(10, int(len(H.nodes()) * 0.15)))
            label_nodes = ranked[:k]
        else:
            label_nodes = ranked[:topk_labels]

    # --- Render two versions
    clean_png = _draw(label_nodes=[], file_suffix="_clean")
    labeled_png = _draw(label_nodes=label_nodes, file_suffix="_labeled")

    print("Saved:", clean_png)
    print("Saved:", labeled_png)

# def plot_network(flow_pairs: pd.DataFrame, title = "Directional flow of traffic"):
#     """
#     Requires networkx to plot traffic in the network
#     """
#     import networkx as nx

#     G = nx.DiGraph()
#     for _, row in flow_pairs.iterrows():
#         G.add_edge(str(row['source_id']), str(row['destination_id']),
#                    weight=row.get('total_value', 1.0))

#     plt.figure()
#     pos = nx.spring_layout(G, seed=42, k=0.3)
#     nx.draw_networkx_nodes(G, pos, node_size=50)

#     # Draw arrows to indicate direction
#     nx.draw_networkx_edges(
#         G, pos,
#         arrows=True,
#         arrowsize=10,
#         width=0.5,
#         alpha=0.5,
#         connectionstyle="arc3,rad=0.08"   # slight curve to show two-way traffic
#     )

#     plt.title(title)
#     plt.axis('off')
#     plt.tight_layout()