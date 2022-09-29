-- Step 4: Get a list of vendor IDs per ASA ID. This list shows the vendor IDs that are caught by the filters of each ASA and takes into account the priority logic of the ASA
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    master_asa_id, -- Artificial ASA ID
    asa_name,
    asa_common_name, -- Artificial ASA name
    is_asa_clustered, -- Flag to determine if the ASA was clustered before or not
    vendor_count_caught_by_asa,
    vendor_code
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
WHERE TRUE
    AND time_condition_id IS NULL -- Filter for records without a time condition because the vendor codes would be duplicated if we have ASA configs with a time condition
    AND customer_condition_id IS NULL -- Filter for records without a customer condition becaue the vendor codes would be duplicated if we have ASA configs with a time condition
;
