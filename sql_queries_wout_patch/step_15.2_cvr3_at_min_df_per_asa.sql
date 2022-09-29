-- Step 15.2: Calculate the overall CVR3 at the smallest DF per ASA so that we can calculate the percentage drop in CVR3 from that base. Also, include the min DF per ASA in this table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS
SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.asa_id,
    a.asa_name,
    a.vendor_count_caught_by_asa,
    b.min_df_total_of_asa,
    a.asa_cvr3_per_df AS asa_cvr3_at_min_df, 
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` a
INNER JOIN (
    SELECT 
        region,
        entity_id,
        country_code,
        asa_id,
        asa_name,
        vendor_count_caught_by_asa,
        MIN(DF_total) AS min_df_total_of_asa 
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code`
    GROUP BY 1,2,3,4,5,6
) b ON a.entity_id = b.entity_id AND a.country_code = b.country_code AND a.asa_id = b.asa_id AND a.DF_total = b.min_df_total_of_asa;