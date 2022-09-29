-- Step 2: Get a list of all cities and zones in each country. This table can be joined to "dps_sessions_mapped_to_ga_sessions" and the "zone_shape" geo field can be used to filter for sessions in the zones that are of interest to us
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code` AS
SELECT
    co.region,
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.name AS zone_name,
    zo.id AS zone_id,
    zo.shape AS zone_shape
FROM `fulfillment-dwh-production.cl.countries` AS co
LEFT JOIN UNNEST(co.platforms) AS p
LEFT JOIN UNNEST(co.cities) AS ci
LEFT JOIN UNNEST(ci.zones) AS zo
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON p.entity_id = ent.entity_id AND co.country_code = ent.country_code
WHERE TRUE
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
;
