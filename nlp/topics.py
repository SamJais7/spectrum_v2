"""Rolling-window topic detection:

1. Every scan, mine terms (hashtags, words, bigrams) from NEW messages into
   term_hourly (per-hour counts).
2. Candidates = terms with >= min_volume posts in the last hour. Velocity =
   (recent - trailing 24h baseline) / baseline.
3. Greedy topic formation: strongest seed term claims co-occurring candidates
   (>=30% of the seed's messages) as its "signature".
4. Assign every window message to the best-matching topic (seed hit or >=2
   signature terms) — identical wording NOT required.
5. Rank by volume & velocity; fire a Viral Alert when a completed hour's rate
   is >= viral_multiplier x the trailing baseline and >= viral_min_posts.
"""

import asyncio
import json
import logging
import re
from collections import defaultdict

from analytics_schema import analytics_connect, get_state, set_state
from ledger import now_us

log = logging.getLogger("collector.topics")
H = 3_600_000_000

STOP = set("""a an and are as at be been but by can cant could did do does dont for from
had has have he her here hers him his how i if in into is it its just like me more most my
no not now of on or our out over own re rt she should so some such than that the their them
then there these they this those to too up us via was we were what when where which who why
will with wont you your youre theyre thats its im ive get got one two people time new today
say says said also about after all am any because been before being between both did does
each even first great had here how into keep last let made make many much never next off
old only other our out over real right same still take tell than thing think want way well
went work year years day days u ur urself gonna wanna every another around back better big
come coming day does everyone going good got guys help here home hour hours id ill isnt it
its ive know life little look looking lot made man maybe mean men mind much need never news
nothing ok okay old put really said same seen since something start started stop sure take
taking talk talking thing things think three today told try trying turn two us use used using
want watch week weeks went what whats when wheres which while whos why will without world
would yeah yes yet youll your youre youtube http https www com tco amp""".split())

_WORD = re.compile(r"[a-z0-9']{3,}")
_TAG = re.compile(r"#([a-z0-9_]{3,})")
_cache = {}          # message_rowid -> term set (avoids re-mining the whole window)


def extract_terms(text: str):
    low = text.lower()
    keep = [t for t in _WORD.findall(low) if t not in STOP]
    terms = set(keep) | set(_TAG.findall(low))
    for i in range(len(keep) - 1):                  # bigrams catch multi-word stories
        terms.add(f"{keep[i]} {keep[i+1]}")
    return terms


def _terms_for(conn, mid, text):
    if mid not in _cache:
        if len(_cache) > 400_000:
            _cache.clear()
        _cache[mid] = extract_terms(text)
    return _cache[mid]


def scan_topics(db_path: str, tcfg: dict) -> dict:
    conn = analytics_connect(db_path)
    try:
        now = now_us()
        win_h = int(tcfg.get("window_hours", 72))
        rec_h = int(tcfg.get("recent_hours", 1))
        base_h = int(tcfg.get("baseline_hours", 24))
        min_vol = int(tcfg.get("min_volume", 5))
        window_start = now - win_h * H
        recent_start = now - rec_h * H

        # 1. mine terms from NEW messages
        wm = int(get_state(conn, "topics_wm", "0"))
        rows = conn.execute("SELECT id, text, posted_at_us FROM messages "
                            "WHERE id>? ORDER BY id LIMIT 50000", (wm,)).fetchall()
        if rows:
            counts = defaultdict(int)
            for mid, text, p in rows:
                hour = (p // H) * H
                for t in _terms_for(conn, mid, text):
                    counts[(t, hour)] += 1
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO term_hourly (term, hour_us, n) VALUES (?,?,?) "
                "ON CONFLICT(term, hour_us) DO UPDATE SET n=n+excluded.n",
                [(t, h, n) for (t, h), n in counts.items()])
            set_state(conn, "topics_wm", rows[-1][0])
            conn.execute("COMMIT")

        # 2. window messages for assignment
        msgs = conn.execute("SELECT id, text, posted_at_us FROM messages "
                            "WHERE posted_at_us>=? ORDER BY posted_at_us LIMIT 200000",
                            (window_start,)).fetchall()
        if not msgs:
            return {"topics": 0}
        msg_terms = {mid: _terms_for(conn, mid, text) for mid, text, _ in msgs}

        # 3. candidate terms: recent volume + velocity
        agg = defaultdict(lambda: [0, 0.0, 0])      # recent, baseline_sum, baseline_hours
        for term, hour, n in conn.execute(
                "SELECT term, hour_us, n FROM term_hourly WHERE hour_us>=?",
                (now - (rec_h + base_h) * H,)):
            a = agg[term]
            if hour >= recent_start:
                a[0] += n
            else:
                a[1] += n
                a[2] += 1
        cands = []
        for term, (rec, bsum, bh) in agg.items():
            if rec >= min_vol:
                base = bsum / bh if bh else 0.0
                vel = (rec - base) / max(base, 1.0)
                cands.append((term, rec, vel, rec * (1 + max(vel, 0))))
        cands.sort(key=lambda c: -c[3])
        cands = cands[:300]
        cand_set = {c[0] for c in cands}

        index = defaultdict(set)
        for mid, terms in msg_terms.items():
            for t in terms & cand_set:
                index[t].add(mid)

        # 4. greedy topic formation
        topics, used = [], set()
        for term, rec, vel, score in cands:
            if term in used:
                continue
            seed_msgs = index[term]
            sig = [term]
            used.add(term)
            for t2, *_ in cands:
                if t2 in used:
                    continue
                if len(index[t2] & seed_msgs) >= 0.3 * len(seed_msgs):
                    sig.append(t2)
                    used.add(t2)
                if len(sig) >= 8:
                    break
            topics.append((term, sig, rec, vel, score))
            if len(topics) >= 30:
                break

        # 5. assign messages (strongest topic wins a message)
        assign = {}
        for key, sig, *_ in sorted(topics, key=lambda t: -t[4]):
            sigset = set(sig)
            for mid, terms in msg_terms.items():
                if mid in assign:
                    continue
                if key in terms or len(terms & sigset) >= 2:
                    assign[mid] = (key, sorted(terms & sigset)[:6])

        # 6+7. persist assignments, hourly counts, alerts — one transaction
        alerts = []
        vmul = float(tcfg.get("viral_multiplier", 10))
        vmin = int(tcfg.get("viral_min_posts", 100))
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM message_topics WHERE message_rowid IN "
                     "(SELECT id FROM messages WHERE posted_at_us>=?)", (window_start,))
        conn.executemany("INSERT INTO message_topics (message_rowid, topic_key, matched_json,"
                         " assigned_at_us) VALUES (?,?,?,?)",
                         [(mid, k, json.dumps(m), now) for mid, (k, m) in assign.items()])
        conn.execute("UPDATE message_analytics SET topic_key=NULL WHERE message_rowid IN "
                     "(SELECT id FROM messages WHERE posted_at_us>=?)", (window_start,))
        conn.executemany("UPDATE message_analytics SET topic_key=? WHERE message_rowid=?",
                         [(k, mid) for mid, (k, _) in assign.items()])
        for key, sig, *_ in topics:
            conn.execute(
                "INSERT INTO topics (topic_key, label, signature_json, first_seen_us,"
                " last_scan_us, active) VALUES (?,?,?,?,?,1)"
                " ON CONFLICT(topic_key) DO UPDATE SET label=excluded.label,"
                " signature_json=excluded.signature_json, last_scan_us=excluded.last_scan_us,"
                " active=1", (key, sig[0], json.dumps(sig), now, now))
        conn.execute("UPDATE topics SET active=0 WHERE topic_key NOT IN "
                     "(SELECT topic_key FROM topics WHERE last_scan_us=?)", (now,))
        conn.execute("DELETE FROM topic_hourly WHERE hour_us>=?", (window_start,))
        conn.execute("INSERT INTO topic_hourly (topic_key, hour_us, n) "
                     "SELECT mt.topic_key, (m.posted_at_us/3600000000)*3600000000, COUNT(*) "
                     "FROM message_topics mt JOIN messages m ON m.id=mt.message_rowid "
                     "WHERE m.posted_at_us>=? GROUP BY 1,2", (window_start,))
        # viral alerts: latest COMPLETED hour vs trailing baseline
        for key, *_ in topics:
            buckets = conn.execute(
                "SELECT hour_us, n FROM topic_hourly WHERE topic_key=? "
                "ORDER BY hour_us DESC LIMIT 8", (key,)).fetchall()
            completed = [(h, n) for h, n in buckets if h + H <= now]
            if len(completed) < 2:
                continue
            cur_h, cur = completed[0]
            prev = [n for _, n in completed[1:]]
            prev_rate = sum(prev) / len(prev)
            if cur >= vmin and cur >= vmul * max(prev_rate, 1.0):
                ratio = cur / max(prev_rate, 1e-9)
                r = conn.execute(
                    "INSERT OR IGNORE INTO viral_alerts (topic_key, hour_us, triggered_at_us,"
                    " prev_rate, new_rate, ratio) VALUES (?,?,?,?,?,?)",
                    (key, cur_h, now, prev_rate, cur, ratio))
                if r.rowcount:
                    alerts.append((key, prev_rate, cur, ratio))
        conn.execute("DELETE FROM term_hourly WHERE hour_us<?", (window_start,))
        conn.execute("COMMIT")
        for key, prev, cur, ratio in alerts:
            log.warning("VIRAL ALERT: topic '%s' jumped %.0f/hr -> %.0f/hr (%.0fx)",
                        key, prev, cur, ratio)
        return {"topics": len(topics), "alerts": alerts}
    finally:
        conn.close()


async def run_topic_loop(cfg, shutdown):
    acfg = cfg.get("analytics", {})
    t = acfg.get("topics", {})
    db = acfg.get("db_path", cfg["storage"]["db_path"])
    wait = int(t.get("scan_seconds", 60))
    log.info("Topic spotter up (scan every %ss)", wait)
    while not shutdown.is_set():
        try:
            scan_topics(db, t)
        except Exception:
            log.exception("topic scan failed")
        for _ in range(max(5, wait)):
            if shutdown.is_set():
                return
            await asyncio.sleep(1)