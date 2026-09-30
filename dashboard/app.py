import os
import time
import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="Bank DQ Dashboard", layout="wide")

DB = {
    "host": os.getenv("DB_HOST", "postgres"),
    "dbname": os.getenv("DB_NAME", "mart"),
    "user": os.getenv("DB_USER", "mart"),
    "password": os.getenv("DB_PASSWORD", "mart"),
    "port": os.getenv("DB_PORT", "5432"),
}

@st.cache_data(ttl=30)
def query(sql, params=None):
    last_exc = None
    for attempt in range(10):
        try:
            with psycopg2.connect(**DB) as conn:
                return pd.read_sql(sql, conn, params=params)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt == 9:
                raise
            time.sleep(2)
    raise last_exc

st.title("Bank Data Quality Dashboard")
st.caption("Учебный DQ / Data Governance MVP — синтетические банковские данные")

try:
    results = query("""
        SELECT run_date, checked_at, rule_code, rule_name, dimension,
               table_name, metric_value, threshold_value, status, details
        FROM dq.check_results
        ORDER BY checked_at DESC
    """)
    catalog = query("""
        SELECT rule_code, rule_name, dimension, criticality, object_name,
               description, owner_role, active
        FROM dq.rule_catalog
        ORDER BY rule_code
    """)
except Exception as exc:
    st.error(f"Не удалось подключиться к mart: {exc}")
    st.stop()

if results.empty:
    st.info("DQ-результаты пока отсутствуют. Сначала запустите DAG data_quality_checks.")
    st.stop()

# check_results - append-only журнал: каждый перезапуск DQ за ту же run_date
# добавляет строки. Для KPI и графиков берём последний результат по каждой
# паре (run_date, rule_code); сырой журнал - в expander ниже.
# results отсортирован по checked_at DESC, поэтому keep="first" = самая свежая.
raw_results = results
results = results.drop_duplicates(subset=["run_date", "rule_code"], keep="first")

# KPI
latest = results.iloc[0]["run_date"]
latest_df = results[results["run_date"] == latest]
passed = int((latest_df["status"] == "PASS").sum())
failed = int((latest_df["status"] == "FAIL").sum())
total = len(latest_df)
quality_pct = round(passed / total * 100, 1) if total else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Последний batch", str(latest))
c2.metric("DQ checks", total)
c3.metric("PASS", passed)
c4.metric("Quality rate", f"{quality_pct}%")

st.divider()

left, right = st.columns(2)
with left:
    st.subheader("Результат по измерениям")
    by_dim = (latest_df.groupby(["dimension", "status"]).size()
              .unstack(fill_value=0).reset_index())
    st.bar_chart(by_dim.set_index("dimension"))

with right:
    st.subheader("Нарушения")
    failures = latest_df[latest_df["status"] == "FAIL"]
    if failures.empty:
        st.success("За последний batch нарушений не обнаружено")
    else:
        st.dataframe(
            failures[["rule_code", "rule_name", "dimension", "table_name", "metric_value", "threshold_value"]],
            use_container_width=True,
            hide_index=True,
        )

st.subheader("История качества")
history = (results.assign(pass_flag=(results.status == "PASS").astype(int))
           .groupby("run_date")
           .agg(checks=("status", "size"), passed=("pass_flag", "sum"))
           .assign(quality_rate=lambda x: x.passed / x.checks * 100))
st.line_chart(history[["quality_rate"]])

with st.expander("Журнал всех прогонов (сырой dq.check_results)"):
    st.dataframe(raw_results, use_container_width=True, hide_index=True)

with st.expander("Каталог DQ-правил"):
    st.dataframe(catalog, use_container_width=True, hide_index=True)

with st.expander("Source-to-Target Mapping"):
    s2t = query("""
        SELECT source_system, source_object, source_column,
               target_object, target_column, transformation,
               business_rule, dq_rule_code, cde, owner_role
        FROM governance.s2t_mapping
        ORDER BY target_object, target_column
    """)
    st.dataframe(s2t, use_container_width=True, hide_index=True)

with st.expander("Data Lineage"):
    lineage = query("""
        SELECT source_layer, source_object, process_name,
               target_layer, target_object, relation_type, description
        FROM governance.data_lineage
        ORDER BY lineage_id
    """)
    st.dataframe(lineage, use_container_width=True, hide_index=True)

# DQ Incidents — заводятся автоматически Airflow-DAG'ом data_quality_checks
# на каждый FAIL (см. data_quality_dag.py, _open_incident), плюс вручную через
# data_governance/incident_demo.sql для отработки полного жизненного цикла.
st.subheader("DQ Incidents")
try:
    incidents = query("""
        SELECT incident_id, opened_at, run_date, rule_code, severity,
               dimension, status, root_cause, owner_role, description
        FROM dq_incident.incidents
        ORDER BY opened_at DESC
    """)
    if incidents.empty:
        st.success("Открытых или исторических DQ-инцидентов нет")
    else:
        open_count = int((~incidents["status"].isin(["RESOLVED", "ACCEPTED"])).sum())
        st.metric("Open incidents", open_count)
        st.dataframe(incidents, use_container_width=True, hide_index=True)
except Exception as exc:
    st.info(f"Incident table is not initialized yet: {exc}")

st.caption("Dashboard предназначен для демонстрации подхода к DQ и Data Governance, а не для production-мониторинга.")
