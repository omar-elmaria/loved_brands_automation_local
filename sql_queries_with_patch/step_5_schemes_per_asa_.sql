-- Step 5: Get a list of scheme IDs per ASA ID
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    vendor_count_caught_by_asa,
    scheme_id,
    CASE WHEN time_condition_id IS NULL AND customer_condition_id IS NULL THEN "Main Scheme" ELSE "Condition Scheme" END AS scheme_type
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
;
