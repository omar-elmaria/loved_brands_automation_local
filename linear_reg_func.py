# Load the packages
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings(action="ignore") # Suppress pandas warnings

# Define a function that fits a linear line through the CVR points
def model(df, cvr_col):
    data_x = df[["df_total"]].values
    data_y = df[[cvr_col]].values
    lm = LinearRegression()
    lm.fit(X=data_x, y=data_y)
    return round(float(np.squeeze(lm.coef_)), 4)

def linear_reg_func(granularity): # "asa" or "vendor"
    # Instantiate the BQ variables to read and write to GCP
    client = bigquery.Client(project="logistics-data-staging-flat")
    bqstorage_client = bigquery_storage.BigQueryReadClient()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    
    if granularity == "asa":
        # Download the datasets
        data_query = """SELECT * FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code`"""
        df = client.query(query=data_query).result().to_dataframe(bqstorage_client=bqstorage_client)
        
        # Get the slopes
        df_slopes = df[df["num_tiers_asa"] > 1].groupby(["entity_id", "country_code", "master_asa_id"]).apply(model, cvr_col = "asa_cvr3_per_df").to_frame(name="asa_cvr3_slope")
        
        # Join the results to the original data frame
        df_merged = pd.merge(left=df, right=df_slopes, on=["entity_id", "country_code", "master_asa_id"], how="left")

        # Destination table name
        destination_tbl = "cvr_per_df_bucket_asa_level_loved_brands_scaled_code"
    elif granularity == "vendor":
        # Download the datasets
        data_query == """SELECT * FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code`"""        
        df = client.query(query=data_query).result().to_dataframe(bqstorage_client=bqstorage_client)
        
        # Get the slopes
        df_slopes = df[df["num_tiers_vendor"] > 1].groupby(["entity_id", "country_code", "master_asa_id", "vendor_code"]).apply(model, cvr_col = "cvr3").to_frame(name="vendor_cvr3_slope")
        
        # Join the results to the original data frame
        df_merged = pd.merge(left=df, right=df_slopes, on=["entity_id", "country_code", "master_asa_id", "vendor_code"], how="left")

        # Destination table name
        destination_tbl = "cvr_per_df_bucket_vendor_level_loved_brands_scaled_code"

    # Upload the df_vendor frame to BQ
    client.load_table_from_dataframe(
        dataframe=df_merged.reset_index(),
        destination=f"dh-logistics-product-ops.pricing.{destination_tbl}",
        job_config=job_config
    ).result()
