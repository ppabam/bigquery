# bigquery load parquet airflow

### Ready
```bash
$ pdm add google-cloud-bigquery
$ cat pyproject.toml | grep requires-python                 
requires-python = ">=3.12,<3.13"
$ pdm add -dG air apache-airflow==2.10.2
```

### Airflow 
#### Version
```bash
$ airflow version
2.10.2
```

#### Dag
![Image](https://github.com/user-attachments/assets/947186b1-fb35-4b2d-93ef-71820c867990)
```bash
start
  │
  ▼
check (BranchPythonVirtualenvOperator)
 ├──────────────────────┐
 ▼                      ▼
load.parquet2big   show.bq.table
         │           │   │
         └───────▶───┘   │
                         ▼
                        end
```

#### TriggerRule
- https://airflow.apache.org/docs/apache-airflow/2.10.3/core-concepts/dags.html#concepts-trigger-rules

| TriggerRule                   | 설명                                             | 실행 조건 예시                      |
|------------------------------|--------------------------------------------------|-------------------------------------|
| `all_success` *(기본값)*     | 모두 성공했을 때만 실행                         | ✅✅ → ✅                            |
| `all_done`                   | 상태 상관없이 모두 끝나면 실행                  | ✅❌⏭ → ✅                           |
| `none_failed`                | 실패만 없으면 실행 (성공+skipped 허용)         | ✅⏭ → ✅ / ✅❌ → ❌                |
| `none_failed_min_one_success` | 실패 없고, 적어도 하나는 성공해야 실행         | ✅⏭ → ✅ / ⏭⏭ → ❌                |




### BigQuery
```sql
-- 중복 날짜 확인
SELECT DISTINCT date
FROM (
  SELECT domain, title, views, size, hour, date, COUNT(*) AS cnt
  FROM load.wiki
  GROUP BY domain, title, views, size, hour, date
  HAVING COUNT(*) > 1
) t ORDER BY date;
```
