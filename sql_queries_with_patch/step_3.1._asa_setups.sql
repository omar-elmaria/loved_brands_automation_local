-- Step 3.1: Get the ASA setups in each country. This table displays the full setup of the ASA including conditions
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS
WITH asa_setup_active_from_max_date_tmp_tbl AS (
    SELECT DISTINCT
        ent.region,
        asa.entity_id,
        asa.country_code,

        h.id AS asa_id,
        h.name AS asa_name,
        h.priority AS asa_priority, -- The priority of the automatic assignment. This does not always match the priority ID in the DPS UI
        TIMESTAMP_TRUNC(h.active_from, SECOND) AS asa_setup_active_from,
        MAX(TIMESTAMP_TRUNC(h.active_from, SECOND)) OVER (PARTITION BY asa.entity_id, asa.country_code, h.id) AS asa_setup_active_from_max_date,
        TIMESTAMP_TRUNC(h.active_to, SECOND) AS asa_setup_active_to,

        asa.vendor_code,
        pc.price_scheme_id AS scheme_id,
        pc.vendor_group_price_config_id,
        pc.priority AS condition_priority,
        sch.id AS time_condition_id,
        cc.id AS customer_condition_id,
        TIMESTAMP_TRUNC(pc.active_from, SECOND) AS pc_setup_active_from,
        MAX(TIMESTAMP_TRUNC(pc.active_from, SECOND)) OVER (PARTITION BY asa.entity_id, asa.country_code, h.id) AS pc_setup_active_from_max_date,
        TIMESTAMP_TRUNC(pc.active_to, SECOND) AS pc_setup_active_to
    FROM `fulfillment-dwh-production.cl.dps_vendor_asa_config_versions` AS asa
    LEFT JOIN UNNEST(asa.dps_automatic_assignment_history) AS h
    LEFT JOIN UNNEST(h.price_config) AS pc
    LEFT JOIN UNNEST(pc.schedule) AS sch
    LEFT JOIN UNNEST(pc.customer_condition) AS cc
    INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON asa.entity_id = ent.entity_id AND asa.country_code = ent.country_code -- Filter only for active DH entities
)

SELECT
    *,
    COUNT(DISTINCT vendor_code) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_caught_by_asa
FROM asa_setup_active_from_max_date_tmp_tbl
WHERE TRUE
    AND asa_setup_active_from = asa_setup_active_from_max_date
    AND asa_setup_active_to IS NULL -- Get the most up-to-date ASA assignment setup
    AND pc_setup_active_from = pc_setup_active_from_max_date
    AND pc_setup_active_to IS NULL -- Get the most up-to-date price configuration setup
ORDER BY region, entity_id, country_code, asa_id, asa_priority, vendor_code, condition_priority, scheme_id
;
