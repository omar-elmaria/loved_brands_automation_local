from google.cloud import bigquery

def cancel_job(job_id, location):
    client = bigquery.Client(project = 'logistics-data-staging-flat')
    job = client.cancel_job(job_id = job_id, location = location)
    print(f"{job.location}:{job.job_id} cancelled")

cancel_job(job_id = '4cd4cae5-9c32-468f-bfbb-8d198f8c6f87', location = 'us')