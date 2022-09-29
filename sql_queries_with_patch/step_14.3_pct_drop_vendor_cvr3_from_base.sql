-- Step 14.3: Calculate the percentage drop in CVR3 from the base/min DF that was calculated in the previous step and append the result to the first table "cvr_per_df_bucket_vendor_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS
SELECT
    a.*,
    b.min_df_total_of_vendor,
    b.vendor_cvr3_at_min_df,
    CASE WHEN a.cvr3 = b.vendor_cvr3_at_min_df THEN NULL ELSE ROUND(a.cvr3 / NULLIF(b.vendor_cvr3_at_min_df, 0) - 1, 4) END AS pct_chng_of_actual_cvr3_from_base
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS a
LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_vendor_level_loved_brands_scaled_code` AS b USING (entity_id, country_code, master_asa_id, vendor_code)
;
