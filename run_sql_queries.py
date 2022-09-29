import re
from datetime import datetime

from google.cloud import bigquery

# Define a common path
common_path = "G:\My Drive\APAC\Autopricing\Vendor Clustering Automation\loved_brands_automation\sql_queries_with_patch"

# Define the percentile ranks
sessions_ntile_thr = 0.75
orders_ntile_thr = 0.75
cvr3_ntile_thr = 0.75

# Define the function that runs a query
def run_query_func(path, suffix):
    # Instantiate a BQ client
    client = bigquery.Client(project="logistics-data-staging-flat")

    # Read the SQL file
    f = open(path + "\\" + suffix, "r")
    sql_script = f.read()
    f.close()

    # Add query parameters if they exist
    if re.findall("step_[0-9]+.[0-9]|step_[0-9]+", suffix)[0] in ["step_10", "step_11"]:
        sql_script = re.sub("sessions_ntile_thr", str(sessions_ntile_thr), sql_script)
        sql_script = re.sub("orders_ntile_thr", str(orders_ntile_thr), sql_script)
        sql_script = re.sub("cvr3_ntile_thr", str(cvr3_ntile_thr), sql_script)
    else:
        pass

    # Run the SQL script
    parent_job = client.query(sql_script).result()

    # Print a success message
    print(
        "The SQL script {} was executed successfully at {} \n".format(
            suffix, datetime.now()
        )
    )


# Run the SQL queries
run_query_func(common_path, "step_1_active_entities.sql")
run_query_func(common_path, "step_2_geo_data.sql")
run_query_func(common_path, "step_3_asa_setups.sql")
run_query_func(common_path, "step_4_vendors_per_asa.sql")
run_query_func(common_path, "step_5_schemes_per_asa.sql")
run_query_func(common_path, "step_6_df_tiers_per_scheme.sql")
run_query_func(common_path, "step_7_df_tiers_per_asa.sql")
run_query_func(common_path, "step_8_ga_session_data.sql")
run_query_func(common_path, "step_9.1_vendor_order_data_for_screening.sql")
run_query_func(common_path, "step_9.2_asa_order_data_for_impact_analysis.sql")
run_query_func(common_path, "step_10_all_metrics_vendor_screening.sql")
run_query_func(common_path, "step_11_filtering_for_vendors_by_pct_ranks.sql")
run_query_func(common_path, "step_12_dps_logs_data.sql")
run_query_func(common_path, "step_13_join_dps_logs_and_ga_sessions.sql")
run_query_func(common_path, "step_14.1_cvr3_per_df_tier_per_vendor.sql")
run_query_func(common_path, "step_14.2_cvr3_at_min_df_per_vendor.sql")
run_query_func(common_path, "step_14.3_pct_drop_vendor_cvr3_from_base.sql")
run_query_func(common_path, "step_15.1_cvr3_per_df_tier_per_asa.sql")
run_query_func(common_path, "step_15.2_cvr3_at_min_df_per_asa.sql")
run_query_func(common_path, "step_15.3_pct_drop_asa_cvr3_from_base.sql")
run_query_func(common_path, "step_15.4_append_asa_tbl_to_vendor_tbl.sql")
run_query_func(common_path, "step_16_final_vendor_list_temp.sql")
run_query_func(common_path, "step_17_insert_new_records_to_final_tbl.sql")
