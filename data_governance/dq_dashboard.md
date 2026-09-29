# DQ Dashboard

Dashboard показывает:

- последний batch;
- число DQ checks;
- количество PASS/FAIL;
- Quality Rate;
- качество по измерениям;
- список нарушений;
- динамику качества по batch date;
- каталог DQ-правил;
- Source-to-Target Mapping;
- Data Lineage.

## Запуск

```bash
docker compose up --build -d
```

После этого:

```text
http://localhost:8501
```

Dashboard читает `mart` напрямую в read-only сценарии приложения и не изменяет
результаты DQ.
