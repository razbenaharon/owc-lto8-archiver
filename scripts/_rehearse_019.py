"""One-off isolated rehearsal of migration 019 against a disposable database.

Not a pytest module -- run directly:
    python scripts/_rehearse_019.py
Requires LTO_TEST_PG_DSN pointed at the disposable server (see AGENTS.md).
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "tests"))

from pg_test_guard import create_test_database, drop_test_database  # noqa: E402
from src.pg_db import PgDatabaseManager  # noqa: E402


def main():
    dbname, conninfo = create_test_database("lto_rehearse_019")
    db = PgDatabaseManager(conninfo)
    try:
        print("== 014 ==")
        db.apply_incremental_scan_schema(finalize=True)
        print("== 015 ==")
        applied = db.apply_container_format_schema()
        assert applied == ["015_postgres_container_formats.sql"], applied
        v1 = db.validate_container_format_schema()
        assert v1["ready"], v1
        print("015 alone: v1 ready =", v1["ready"])

        auth = db.validate_container_format_authority()
        assert auth is v1 or auth["ready"], auth
        print("dispatcher on 015-only: OK (v1 path)")

        print("== 016+017 ==")
        applied = db.apply_stored_tar_plan_schema()
        assert applied == [
            "016_postgres_stored_tar_plans.sql",
            "017_postgres_stored_tar_publication.sql"], applied

        v1_after = db.container_format_schema_report()
        print("v1 report after 016/017: ready =", v1_after["ready"],
              "issues =", v1_after["issues"])
        assert not v1_after["ready"], "expected v1 to show drift (known defect)"

        try:
            db.validate_container_format_authority()
            raise SystemExit("expected dispatcher to fail closed before 019")
        except RuntimeError as exc:
            assert "v2 schema authority" in str(exc), exc
            print("dispatcher fails closed before 019: OK ->", exc)

        print("== 019 ==")
        applied = db.apply_container_format_schema_authority_v2()
        assert applied == [
            "019_postgres_container_format_schema_authority_v2.sql"], applied
        v2 = db.validate_container_format_schema_authority_v2()
        assert v2["ready"], v2
        print("019 applied: v2 ready =", v2["ready"])

        auth2 = db.validate_container_format_authority()
        assert auth2["installed"] and auth2["ready"], auth2
        print("dispatcher after 019: OK (v2 path)")

        print("== idempotency ==")
        again = db.apply_container_format_schema_authority_v2()
        assert again == [
            "019_postgres_container_format_schema_authority_v2.sql"], again
        v2_again = db.validate_container_format_schema_authority_v2()
        assert v2_again["row"]["authority_id"] == v2["row"]["authority_id"], (
            "re-apply must not create a second row")
        print("idempotent re-apply: OK, same authority_id =",
              v2_again["row"]["authority_id"])

        print("== immutability ==")
        try:
            with db._pool.connection() as conn:
                conn.execute(
                    "UPDATE container_format_schema_authority "
                    "SET schema_fingerprint='x' WHERE authority_version=2")
                conn.commit()
            raise SystemExit("expected UPDATE to be rejected")
        except Exception as exc:  # noqa: BLE001
            assert "immutable" in str(exc), exc
            print("UPDATE rejected: OK ->", str(exc).splitlines()[0])

        try:
            with db._pool.connection() as conn:
                conn.execute(
                    "DELETE FROM container_format_schema_authority "
                    "WHERE authority_version=2")
                conn.commit()
            raise SystemExit("expected DELETE to be rejected")
        except Exception as exc:  # noqa: BLE001
            assert "immutable" in str(exc), exc
            print("DELETE rejected: OK ->", str(exc).splitlines()[0])

        print("\nALL REHEARSAL CHECKS PASSED")
    finally:
        db.close()
        drop_test_database(dbname)


if __name__ == "__main__":
    main()
