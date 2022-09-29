-- Step 7: Get the DF tiers of each ASA. We will need this in step 14 when we implement the LBs logic
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,
    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    fee
FROM `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
ORDER BY 1, 2, 3, 4, 5
;
