-- Step 3.3: Add the master_asa_id to the table that contains the ASA setups in each country
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS
SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.asa_id,
    CASE WHEN b.is_asa_clustered = TRUE THEN MAX(asa_id) OVER(PARTITION BY a.entity_id, a.country_code) + 1 ELSE a.asa_id END AS master_asa_id,
    a.asa_name,
    COALESCE(b.asa_common_name, a.asa_name) AS asa_common_name,
    COALESCE(b.is_asa_clustered, FALSE) AS is_asa_clustered,
    a.asa_priority, -- The priority of the automatic assignment. This does not always match the priority ID in the DPS UI
    a.asa_setup_active_from,
    a.asa_setup_active_from_max_date,
    a.asa_setup_active_to,
    a.vendor_code,
    a.scheme_id,
    a.vendor_group_price_config_id,
    a.condition_priority,
    a.time_condition_id,
    a.customer_condition_id,
    a.pc_setup_active_from,
    a.pc_setup_active_from_max_date,
    a.pc_setup_active_to,
    a.vendor_count_caught_by_asa
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS a
LEFT JOIN `dh-logistics-product-ops.pricing.parent_child_asa_linking_loved_brands_scaled_code` AS b USING (entity_id, country_code, asa_id)
;
