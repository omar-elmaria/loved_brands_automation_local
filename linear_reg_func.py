# Load the packages
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings(action="ignore") # Suppress pandas warnings

# Download the datasets that contains the asa and vendor level CVRs
client = bigquery.Client(project="logistics-data-staging-flat")
bqstorage_client = bigquery_storage.BigQueryReadClient()
vendor_query = """SELECT * FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code`"""
asa_query = """SELECT * FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code`"""

# Download the datasets
df_vendor = client.query(query=vendor_query).result().to_dataframe(bqstorage_client=bqstorage_client)
df_asa = client.query(query=asa_query).result().to_dataframe(bqstorage_client=bqstorage_client)

# Define a function that fits a linear line through the CVR points
def model(df, cvr_col):
    data_x = df[["df_total"]].values
    data_y = df[[cvr_col]].values
    lm = LinearRegression()
    lm.fit(X=data_x, y=data_y)
    return round(float(np.squeeze(lm.coef_)), 4)

df_vendor_slopes = df_vendor[df_vendor["num_tiers_vendor"] > 1].groupby(["entity_id", "country_code", "master_asa_id", "vendor_code"]).apply(model, cvr_col = "cvr3").to_frame(name="vendor_cvr3_slope")
df_asa_slopes = df_asa[df_asa["num_tiers_asa"] > 1].groupby(["entity_id", "country_code", "master_asa_id"]).apply(model, cvr_col = "asa_cvr3_per_df").to_frame(name="asa_cvr3_slope")

# Join the results to the original data frame
df_vendor_merged = pd.merge(left=df_vendor, right=df_vendor_slopes, on=["entity_id", "country_code", "master_asa_id", "vendor_code"], how="left")
df_asa_merged = pd.merge(left=df_asa, right=df_asa_slopes, on=["entity_id", "country_code", "master_asa_id"], how="left")

# Upload the new data frames to BQ
job_config = bigquery.LoadJobConfig()

# Set the job_config to overwrite the data in the table
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

# Upload the df_vendor frame to BQ
job1 = client.load_table_from_dataframe(
    dataframe=df_vendor_merged.reset_index(),
    destination="dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code",
    job_config=job_config
).result()

# Upload the df_asa frame to BQ
job2 = client.load_table_from_dataframe(
    dataframe=df_asa_merged.reset_index(),
    destination="dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code",
    job_config=job_config
).result()