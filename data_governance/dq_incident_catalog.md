# DQ Incident Scenarios

Проект содержит намеренно воспроизводимые DQ-инциденты.
Цель — показать полный цикл Data Quality:

```text
bad data
   ↓
DQ detection
   ↓
incident
   ↓
classification / root cause
   ↓
quarantine
   ↓
remediation
   ↓
re-run
   ↓
PASS
```

## INC-001 — Completeness

**Симптом:** `fact_transactions.client_id IS NULL`.

- Dimension: Completeness
- Severity: HIGH
- Likely root cause: обязательное поле потеряно на этапе source extraction/mapping.
- Action: запись помещается в quarantine; проверяется source payload и S2T mapping.
- Acceptance: после исправления `DQ-006` = PASS.

## INC-002 — Validity

**Симптом:** отрицательная сумма операции.

- Dimension: Validity
- Severity: HIGH
- Likely root cause: некорректное значение в source или отсутствие domain validation.
- Action: quarantine; проверка source record и бизнес-правила amount.
- Acceptance: `DQ-003` = PASS.

## INC-003 — Uniqueness

**Симптом:** попытка вставить строку с уже существующим `transaction_id`.

- Dimension: Uniqueness
- Severity: HIGH
- `transaction_id` — `PRIMARY KEY`, поэтому реальный дубль в таблице невозможен:
  первый рубеж защиты — сам constraint (Postgres отклонит вставку с
  `unique_violation`). `generate_dq_incidents.sql` явно перехватывает это
  исключение и заводит инцидент — демонстрация defense-in-depth
  (PK + DQ-002), а не имитация "тихого" прохождения дубля в mart.
- Likely root cause: повторная загрузка batch / отсутствие idempotency
  где-то до PK (например, при `TRUNCATE + COPY` в обход constraint'ов).
- Action: проверить batch key и загрузочную логику.
- Acceptance: `DQ-002` = PASS (по построению — раз PK не даёт дублю попасть
  в таблицу, эта проверка структурно не должна проваливаться).

## INC-004 — Consistency

**Симптом:** `account_id` отсутствует в `dim_account`.

- Dimension: Consistency
- Severity: HIGH
- Likely root cause: неполная загрузка справочника или нарушение порядка загрузки.
- Action: quarantine; проверить completeness/reference-data freshness.
- Acceptance: `DQ-005` = PASS.

## INC-005 — Freshness

**Симптом:** ожидаемый batch за дату не появился.

- Dimension: Freshness
- Severity: HIGH
- Likely root cause: upstream source delay / failed pipeline.
- Action: incident; проверить upstream SLA и Airflow task status.
- Acceptance: после появления batch `DQ-010` = PASS.

## Incident lifecycle

| Status | Meaning |
|---|---|
| OPEN | нарушение обнаружено |
| TRIAGED | определён тип дефекта и зона ответственности |
| IN_REMEDIATION | выполняется исправление |
| RECHECK | DQ запускается повторно |
| RESOLVED | нарушение устранено |
| ACCEPTED | результат подтверждён Data Steward |

## Root-cause categories

- SOURCE_DATA — дефект пришёл из источника;
- MAPPING — ошибка source-to-target mapping;
- TRANSFORMATION — ошибка трансформации;
- REFERENCE_DATA — проблема справочника;
- PIPELINE — технический сбой загрузки;
- DUPLICATION — повторная загрузка/неидемпотентность;
- UNKNOWN — причина пока не установлена.
