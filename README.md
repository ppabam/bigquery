# bigquery load parquet airflow

### Ready
```bash
$ pdm add google-cloud-bigquery
$ cat pyproject.toml | grep requires-python                 
requires-python = ">=3.12,<3.13"
$ pdm add -dG air apache-airflow==2.10.2
```
