"""End-to-end sanity check: validate → vault → ledger → tamper resistance.

    python smoke_test.py
"""
import os
import tempfile
from datetime import datetime, timedelta, timezone

import validation
from ledger import Ledger, row_hash
from models import NormalizedMessage
from storage import Vault


def main():
    tmp = tempfile.mkdtemp(prefix="vault-smoke-")
    db, anchor = os.path.join(tmp, "vault.db"), os.path.join(tmp, "ledger.anchor")
    ledger = Ledger(anchor)
    vault = Vault({"db_path": db, "security": {"enforce_immutable_messages": True}}, ledger)
    t = datetime.now(timezone.utc) - timedelta(minutes=5)

    def msg(eid, text, **kw):
        return NormalizedMessage(
            source="x",
            external_id=eid,
            conversation_id="1",
            author_id="100",
            author_username="alice",
            text=text,
            posted_at=t,
            reply_to_external_id=kw.pop("reply_to_external_id", None),
            **kw,
        )

    # 1. two new messages → 2 rows, 1 sealed ledger block
    r = vault.write_batch([msg("1", "launch day, let's go!! 🚀", metrics={"likes": 3}),
                           msg("2", "this is terrible and you should feel bad")])
    assert r.inserted == 2 and r.blocks == 1, f"unexpected: {r}"

    # 2. same post again, bigger numbers → metrics update, NO duplicate row
    r = vault.write_batch([msg("1", "launch day, let's go!! 🚀", metrics={"likes": 41})])
    assert r.updated == 1 and r.inserted == 0, f"unexpected: {r}"

    # 3. garbage → rejected, never stored
    bad = validation.validate(msg("3", "   "))
    assert not bad.ok and "empty" in bad.reason
    vault.record_rejection(msg("3", "   "), bad.reason)

    # 4. chain intact + every stored row matches its cryptographic receipt
    ok, why = ledger.verify_chain(vault._conn)
    assert ok, why
    for rid, h, src, eid, conv, aid, auser, text, p_us, reply in vault._conn.execute(
            "SELECT e.message_rowid, e.message_hash, m.source, m.external_id,"
            " m.conversation_id, m.author_id, m.author_username, m.text,"
            " m.posted_at_us, m.reply_to_external_id FROM ledger_entries e"
            " JOIN messages m ON m.id=e.message_rowid"):
        assert row_hash((src, eid, conv, aid, auser, text, p_us, reply)) == h, f"rowid {rid}"

    # 5. direct tampering is physically blocked by the database itself
    try:
        vault._conn.execute("UPDATE messages SET text='FORGED' WHERE external_id='1'")
        raise SystemExit("FAIL: tampering was allowed!")
    except Exception as e:
        assert "immutable" in str(e).lower(), e

    n = vault._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    rev = vault._conn.execute("SELECT revision FROM messages WHERE external_id='1'").fetchone()[0]
    print("OK ✔")
    print(f"  messages stored: {n} (re-post produced an update, not a duplicate; revision={rev})")
    print(f"  ledger: chain VERIFIED from genesis, head block #{ledger.head(vault._conn)[0]}")
    print(f"  receipts: every row matches its SHA-256 fingerprint")
    print(f"  tamper test: UPDATE blocked by trigger")
    print(f"  scratch files: {tmp} (safe to delete)")
    vault.close()


if __name__ == "__main__":
    main()