#!/usr/bin/env python3
"""Проверка: метрики ведения метаданных, снимки, права ролей, задача Airflow, Grafana.

Запуск из корня репозитория при поднятом стеке:
    python3 scripts/check_metadata_quality.py

Нужны только docker compose и python3. Состояние не меняется: тестовый снимок
пишется за 2026-09-01 (start_date DAG'а, вне демо-дат) и сразу удаляется,
проверка записи под ролью mart идёт в транзакции с ROLLBACK.
Если логин Grafana меняли: GRAFANA_USER=... GRAFANA_PASSWORD=... python3 scripts/check_metadata_quality.py
"""
import base64
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")
GRAFANA_AUTH = base64.b64encode(
    f"{os.environ.get('GRAFANA_USER', 'admin')}:{os.environ.get('GRAFANA_PASSWORD', 'admin')}".encode()
).decode()
DATASOURCE_UID = "mart-pg"
DASHBOARD_UID = "bank-dq-metadata"
TEST_DATE = "2026-09-01"
DASHBOARD = {}


def sh(*cmd):
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()


def sql(query, user="airflow", password=None):
    """psql в контейнере postgres. Без пароля — локальный сокет под суперпользователем
    init-скрипта; с паролем — по TCP, чтобы сработала парольная аутентификация роли.
    VERBOSITY=verbose добавляет SQLSTATE в текст ошибки: 42501 — нет прав."""
    cmd = ["docker", "compose", "exec", "-T"]
    if password:
        cmd += ["-e", f"PGPASSWORD={password}"]
    cmd += ["postgres", "psql", "-U", user, "-d", "mart", "-Atq",
            "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]
    if password:
        cmd += ["-h", "localhost"]
    return sh(*cmd, "-c", query)


def grafana(path, body=None):
    req = urllib.request.Request(
        GRAFANA_URL + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Basic {GRAFANA_AUTH}", "Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, raw.decode(errors="replace")[:300]


def check_containers():
    rc, out = sh("docker", "compose", "ps", "--services", "--status", "running")
    if rc:
        return False, out
    missing = {"postgres", "airflow-scheduler", "grafana"} - set(out.split())
    return not missing, f"не запущены: {', '.join(sorted(missing))} — docker compose up -d" if missing else ""


def check_db_objects():
    rc, out = sql("""
        select (select count(*) from pg_views
                 where schemaname || '.' || viewname in (
                     'governance.v_catalog_object', 'governance.v_metadata_quality',
                     'governance.v_object_quality', 'governance.v_metadata_gaps', 'dq.v_daily_quality'))
            || ' ' || (select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace
                        where n.nspname = 'governance' and p.proname = 'take_metadata_snapshot')
            || ' ' || (select count(*) from pg_roles where rolname = 'grafana_ro')""")
    ok = rc == 0 and out == "5 1 1"
    return ok, "" if ok else f"{out} вместо «5 1 1» — накати mart/metadata_quality.sql под -U airflow"


def check_perimeter():
    rc, out = sql("select count(*) filter (where object_level = 'table') || ' ' "
                  "|| count(*) filter (where object_level = 'column') from governance.v_catalog_object")
    ok = rc == 0 and out == "4 30"
    return ok, "" if ok else f"таблиц и колонок: {out}, ожидалось 4 и 30"


def check_metrics():
    rc, out = sql("select count(*) || '|' || string_agg(metric_code || ' ' || coalesce(value_pct::text, '—'), "
                  "', ' order by metric_code) from governance.v_metadata_quality")
    if rc:
        return False, out
    n, values = out.split("|", 1)
    return n == "5" and "—" not in values, values


def check_snapshots():
    rc, out = sql("select count(distinct snapshot_date) from governance.metadata_quality_snapshot "
                  f"where snapshot_date <> '{TEST_DATE}'")
    if rc:
        return False, out
    n = int(out)
    hint = "" if n >= 3 else " — для кривой на графике запусти data_governance/metadata_quality_demo.sql"
    return n >= 3, f"дат со снимками: {n}{hint}"


def check_grafana_ro():
    rc1, out1 = sql("select count(*) from governance.v_metadata_quality", "grafana_ro", "grafana_ro")
    rc2, out2 = sql(f"begin; select governance.take_metadata_snapshot('{TEST_DATE}'); rollback;",
                    "grafana_ro", "grafana_ro")
    rc3, out3 = sql("select count(*) from restricted.dim_client_pii", "grafana_ro", "grafana_ro")
    problems = []
    if not (rc1 == 0 and out1 == "5"):
        problems.append(f"не читает метрики: {out1}")
    if not (rc2 != 0 and "42501" in out2):
        problems.append("может писать снимки" if rc2 == 0 else f"неожиданная ошибка: {out2}")
    if not (rc3 != 0 and "42501" in out3):
        problems.append("видит restricted" if rc3 == 0 else f"неожиданная ошибка: {out3}")
    return not problems, "; ".join(problems)


def check_mart_writes():
    rc, out = sql(f"begin; select governance.take_metadata_snapshot('{TEST_DATE}'); rollback;", "mart", "mart")
    ok = rc == 0 and "5" in out.split()
    return ok, "" if ok else out


def check_dag():
    code = (
        "from airflow.models import DagBag\n"
        "b = DagBag(include_examples=False)\n"
        "d = b.get_dag('data_quality_checks')\n"
        "t = d.get_task('take_metadata_snapshot') if d and d.has_task('take_metadata_snapshot') else None\n"
        "print('RESULT', len(b.import_errors), t.trigger_rule if t else '-',"
        " ','.join(sorted(t.upstream_task_ids)) if t else '-')\n"
    )
    rc, out = sh("docker", "compose", "exec", "-T", "airflow-scheduler", "python", "-c", code)
    line = next((l for l in out.splitlines() if l.startswith("RESULT ")), "")
    ok = line == "RESULT 0 all_done run_data_quality_checks"
    return ok, "" if ok else (line or out[-400:]) + " — docker compose exec airflow-scheduler airflow dags list-import-errors"


def check_task_test():
    rc, out = sh("docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", "tasks", "test",
                 "data_quality_checks", "take_metadata_snapshot", TEST_DATE)
    success = rc == 0 and "Marking task as SUCCESS" in out
    _, n = sql(f"select count(*) from governance.metadata_quality_snapshot where snapshot_date = '{TEST_DATE}'")
    sql(f"delete from governance.metadata_quality_snapshot where snapshot_date = '{TEST_DATE}'")
    ok = success and n == "5"
    return ok, f"снимок за {TEST_DATE}: {n} метрик, удалён" if ok else out[-600:]


def check_grafana_health():
    status, body = grafana("/api/health")
    ok = status == 200 and isinstance(body, dict) and body.get("database") == "ok"
    return ok, f"версия {body.get('version')}" if ok else f"HTTP {status}: {body}"


def check_datasource():
    status, body = grafana(f"/api/datasources/uid/{DATASOURCE_UID}/health")
    ok = status == 200 and isinstance(body, dict) and body.get("status") == "OK"
    return ok, body.get("message", "") if isinstance(body, dict) else f"HTTP {status}: {body}"


def check_dashboard():
    status, body = grafana(f"/api/dashboards/uid/{DASHBOARD_UID}")
    if status != 200 or not isinstance(body, dict):
        return False, f"HTTP {status} — docker compose logs grafana | grep -iE 'error|provision'"
    DASHBOARD.update(body["dashboard"])
    panels = DASHBOARD.get("panels", [])
    return len(panels) == 5, f"«{DASHBOARD.get('title')}», панелей: {len(panels)}"


def check_panel_queries():
    if not DASHBOARD:
        return False, "дашборд не загружен — см. проверку выше"
    ok, report = True, []
    for p in DASHBOARD["panels"]:
        t = p["targets"][0]
        status, body = grafana("/api/ds/query", {
            "from": "now-90d", "to": "now",
            "queries": [{"refId": "A", "datasource": {"uid": DATASOURCE_UID}, "rawSql": t["rawSql"],
                         "format": t["format"], "rawQuery": True, "editorMode": "code"}],
        })
        res = body.get("results", {}).get("A", {}) if isinstance(body, dict) else {}
        if status != 200 or res.get("error"):
            ok = False
            report.append(f"#{p['id']} ошибка: {res.get('error') or body}")
            continue
        rows = sum(len(f["data"]["values"][0]) for f in res.get("frames", []) if f.get("data", {}).get("values"))
        report.append(f"#{p['id']}: {rows}")
    return ok, "строк по панелям " + ", ".join(report)


CHECKS = [
    ("Контейнеры postgres, airflow-scheduler, grafana запущены", check_containers),
    ("Объекты БД: 5 вьюх, функция снимка, роль grafana_ro", check_db_objects),
    ("Периметр каталога: 4 таблицы, 30 колонок", check_perimeter),
    ("Метрики ведения метаданных считаются", check_metrics),
    ("Снимки метрик для графика динамики", check_snapshots),
    ("grafana_ro читает метрики, не пишет снимки, не видит restricted", check_grafana_ro),
    ("mart (роль Airflow) пишет снимок", check_mart_writes),
    ("Airflow: DAG без ошибок импорта, снимок после DQ с all_done", check_dag),
    ("Airflow: tasks test take_metadata_snapshot", check_task_test),
    ("Grafana: сервис отвечает", check_grafana_health),
    ("Grafana: источник mart подключается под grafana_ro", check_datasource),
    ("Grafana: дашборд загружен из provisioning", check_dashboard),
    ("Grafana: SQL всех панелей выполняется через источник", check_panel_queries),
]


def main():
    print("Проверка: метрики ведения метаданных + Grafana\n")
    failed = 0
    for name, fn in CHECKS:
        try:
            ok, detail = fn()
        except Exception as e:  # docker/сеть недоступны, неожиданный ответ
            ok, detail = False, f"{type(e).__name__}: {e}"
        failed += not ok
        print(f"  {'OK  ' if ok else 'FAIL'}  {name}")
        if detail:
            print(f"        {detail}")
    print(f"\nИтог: {len(CHECKS) - failed} OK, {failed} FAIL")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
