-- Step 15.3: Calculate the percentage drop in CVR3 from the base/min DF that was calculated in the previous step and append the result to the first table "cvr_per_df_bucket_asa_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS
WITH add_tier_rank AS (
    SELECT
        a.*,
        b.min_df_total_of_asa,
        b.asa_cvr3_at_min_df,
        CASE WHEN a.asa_cvr3_per_df = b.asa_cvr3_at_min_df THEN NULL ELSE ROUND(a.asa_cvr3_per_df / NULLIF(b.asa_cvr3_at_min_df, 0) - 1, 4) END AS pct_chng_of_asa_cvr3_from_base,
        ROW_NUMBER() OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id ORDER BY a.df_total) AS tier_rank_asa
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS a
    LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS b USING (entity_id, country_code, master_asa_id)
)

SELECT 
    *,
    MAX(tier_rank_asa) OVER (PARTITION BY entity_id, country_code, master_asa_id) AS num_tiers_asa
FROM add_tier_rank
;
