"""Influence & community math (pure Python, no numpy):

* PageRank (weighted, power iteration)      -> "megaphone" ranking (with in-degree)
* Brandes betweenness (weighted)            -> "bridge" ranking, capped node count
* Label propagation (weighted, deterministic) -> communities
* Botnet suspicion: communities whose edge weight is ~all internal AND dense
  (clique-like) AND >= min_size — classic coordinated-cluster signature.
"""

import heapq
import logging
import time
from collections import defaultdict

from analytics_schema import analytics_connect
from ledger import now_us

log = logging.getLogger("collector.graph.analysis")
H = 3_600_000_000


def _pagerank(nodes, out_adj, damping=0.85, iters=60, tol=1e-8):
    n = len(nodes)
    if not n:
        return {}
    pr = {v: 1 / n for v in nodes}
    dout = {v: sum(out_adj[v].values()) for v in nodes}
    for _ in range(iters):
        dangling = sum(pr[v] for v in nodes if not dout[v])
        new = {v: (1 - damping) / n + damping * dangling / n for v in nodes}
        for v in nodes:
            if dout[v]:
                share = damping * pr[v] / dout[v]
                for u, w in out_adj[v].items():
                    new[u] += share * w
        if sum(abs(new[v] - pr[v]) for v in nodes) < tol:
            pr = new
            break
        pr = new
    return pr


def _betweenness(und, cap):
    nodes = sorted(und, key=lambda v: (-sum(und[v].values()), v))
    if len(nodes) > cap:                       # bounded: top-cap induced subgraph
        keep = set(nodes[:cap])
        und = {u: {v: w for v, w in und[u].items() if v in keep} for u in keep}
        nodes = [v for v in nodes if v in keep]
        log.warning("betweenness capped to %d nodes", cap)
    bc = defaultdict(float)
    for s in nodes:
        S, P, sigma, dist = [], defaultdict(list), defaultdict(float, {s: 1.0}), {s: 0.0}
        pq = [(0.0, s)]
        while pq:
            d, v = heapq.heappop(pq)
            if d > dist.get(v, float("inf")):
                continue
            S.append(v)
            for u, w in und[v].items():
                nd = d + 1.0 / max(w, 1e-9)    # strong tie = short distance
                if u not in dist or nd < dist[u]:
                    dist[u], sigma[u], P[u] = nd, sigma[v], [v]
                    heapq.heappush(pq, (nd, u))
                elif nd == dist[u]:
                    sigma[u] += sigma[v]
                    P[u].append(v)
        delta = defaultdict(float)
        for w in reversed(S):
            for v in P[w]:
                delta[v] += sigma[v] / sigma[w] * (1 + delta[w])
            if w != s:
                bc[w] += delta[w]
    return {v: bc[v] / 2 for v in nodes}


def _label_propagation(und):
    order = sorted(und, key=lambda v: (-sum(und[v].values()), v))
    lab = {v: i for i, v in enumerate(order)}
    for _ in range(30):
        changed = False
        for v in order:
            counts = defaultdict(float)
            for u, w in und[v].items():
                counts[lab[u]] += w
            if not counts:
                continue
            best = min(counts, key=lambda l: (-counts[l], l))
            if best != lab[v]:
                lab[v], changed = best, True
        if not changed:
            break
    ids, out = {}, {}
    for v in order:
        ids.setdefault(lab[v], len(ids))
        out[v] = ids[lab[v]]
    return out


def compute_all(db_path: str, gcfg: dict):
    t0 = time.monotonic()
    window_us = now_us() - int(gcfg.get("window_hours", 168)) * H
    cap = int(gcfg.get("betweenness_max_nodes", 20000))
    bn = gcfg.get("botnet", {})
    min_size = int(bn.get("min_size", 5))
    thr_ratio = float(bn.get("internal_ratio", 0.90))
    thr_dens = float(bn.get("density", 0.60))

    conn = analytics_connect(db_path)
    try:
        per_source = defaultdict(lambda: {"out": defaultdict(dict), "in": defaultdict(dict),
                                          "und": defaultdict(dict)})
        for source, fa, ta, w in conn.execute(
                "SELECT source, from_author, to_author, weight FROM graph_edges"
                " WHERE last_us>=?", (window_us,)):
            g = per_source[source]
            g["out"][fa][ta] = g["out"][fa].get(ta, 0) + w
            g["in"][ta][fa] = g["in"][ta].get(fa, 0) + w
            g["und"][fa][ta] = g["und"][fa].get(ta, 0) + w
            g["und"][ta][fa] = g["und"][ta].get(fa, 0) + w

        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM graph_metrics")
        conn.execute("DELETE FROM communities")
        t = now_us()
        total_nodes = total_flagged = 0
        for source, g in per_source.items():
            nodes = set(g["und"])
            total_nodes += len(nodes)
            pr = _pagerank(nodes, g["out"])
            bt = _betweenness({v: dict(g["und"][v]) for v in nodes}, cap)
            comm = _label_propagation({v: dict(g["und"][v]) for v in nodes})

            members = defaultdict(list)
            for v, c in comm.items():
                members[c].append(v)
            internal_w = defaultdict(float)
            external_w = defaultdict(float)
            pairs = defaultdict(set)
            neigh_comm = defaultdict(set)
            for u in nodes:
                cu = comm[u]
                for v, w in g["und"][u].items():
                    if u >= v:                                  # count each undirected edge once
                        continue
                    cv = comm[v]
                    if cu == cv:
                        internal_w[cu] += w
                        pairs[cu].add((u, v))
                    else:
                        external_w[cu] += w
                        external_w[cv] += w
                        neigh_comm[u].add(cv)
                        neigh_comm[v].add(cu)

            if bt:
                vals = sorted(bt.values())
                p95 = vals[int(0.95 * (len(vals) - 1))]
            else:
                p95 = 0.0
            conn.executemany(
                "INSERT INTO graph_metrics (source, author_id, in_w, out_w, pagerank,"
                " betweenness, community, bridge, computed_at_us) VALUES (?,?,?,?,?,?,?,?,?)",
                [(source, v,
                  sum(g["in"][v].values()), sum(g["out"][v].values()),
                  round(pr.get(v, 0), 8), round(bt.get(v, 0), 3), comm[v],
                  1 if bt.get(v, 0) >= p95 and bt.get(v, 0) > 0 and len(neigh_comm[v]) >= 2 else 0,
                  t) for v in nodes])
            for c, mem in members.items():
                n = len(mem)
                iw, ew = internal_w[c], external_w[c]
                ratio = iw / (iw + ew) if (iw + ew) else 0.0
                density = (len(pairs[c]) * 2) / (n * (n - 1)) if n > 1 else 0.0
                susp = round(ratio * min(1.0, density / 0.5), 3)
                flagged = 1 if (n >= min_size and ratio >= thr_ratio
                                and density >= thr_dens) else 0
                total_flagged += flagged
                conn.execute(
                    "INSERT INTO communities (source, community, size, internal_w, external_w,"
                    " internal_ratio, density, suspicion, flagged, computed_at_us)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (source, c, n, iw, ew, round(ratio, 3), round(density, 3), susp,
                     flagged, t))
        conn.execute("COMMIT")
        log.info("graph analysis: %d nodes across %d sources, %d suspicious clusters "
                 "flagged (%.1fs)", total_nodes, len(per_source), total_flagged,
                 time.monotonic() - t0)
    finally:
        conn.close()