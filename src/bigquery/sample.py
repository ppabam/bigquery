from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = ('SELECT title FROM `praxis-bond-455400-a4.temp.t20240101` LIMIT 10')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = ('SELECT title FROM `praxis-bond-455400-a4.temp.t20240101` LIMIT 10')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name))