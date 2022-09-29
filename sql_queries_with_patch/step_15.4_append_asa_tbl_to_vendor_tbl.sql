-- Step 15.4: Append the ASA level table "cvr_per_df_bucket_asa_level_loved_brands_scaled_code" to the vendor level table "cvr_per_df_bucket_vendor_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_plus_cvr_thresholds_loved_brands_scaled_code` AS
SELECT
    -- Vendor level data
    a.* EXCEPT (cvr3, min_df_total_of_vendor, vendor_cvr3_at_min_df, pct_chng_of_actual_cvr3_from_base),
    a.min_df_total_of_vendor,
    a.vendor_cvr3_at_min_df,

    -- ASA level data at the min DF tier of the ASA
    b.min_df_total_of_asa, -- The min DF tier of the ASA
    b.asa_cvr3_at_min_df, -- CVR3 at the lowest DF tier of the ASA, not the lowest DF observed in the vendor's sessions

    -- ASA level data at each DF tier of the ASA. We only match to the DF tiers observed in the vendor's sessions
    c.asa_cvr3_per_df,
    c.pct_chng_of_asa_cvr3_from_base,

    -- Vendor level data
    a.cvr3,
    a.pct_chng_of_actual_cvr3_from_base, -- The base here being the lowest DF tier observed in the vendor's sessions NOT the lowest DF of the ASA
    -- If the change in the vendor's CVR is > the change in the ASA's CVR, that's a YES. Otherwise, NO
    CASE WHEN a.pct_chng_of_actual_cvr3_from_base > c.pct_chng_of_asa_cvr3_from_base THEN "Y" ELSE "N" END AS is_lb_test_passed,
    -- The # of YES's per vendor
    SUM(CASE WHEN a.pct_chng_of_actual_cvr3_from_base > c.pct_chng_of_asa_cvr3_from_base THEN 1 ELSE 0 END) OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id, a.vendor_code) AS num_yes
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS a
LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS b
    ON TRUE
        AND a.entity_id = b.entity_id
        AND a.country_code = b.country_code
        AND a.master_asa_id = b.master_asa_id
LEFT JOIN `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS c
    ON TRUE
        AND a.entity_id = c.entity_id
        AND a.country_code = c.country_code
        AND a.master_asa_id = c.master_asa_id
        AND a.df_total = c.df_total
;
