-- Step 1: Obtain a list of all active entities in Delivery Hero
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS
WITH dps AS (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`)

SELECT
    ent.region,
    p.entity_id,
    LOWER(ent.country_iso) AS country_code,
    ent.country_name
FROM `fulfillment-dwh-production.cl.entities` AS ent
LEFT JOIN UNNEST(platforms) AS p
INNER JOIN dps ON p.entity_id = dps.entity_id
WHERE TRUE
    AND p.entity_id NOT LIKE "ODR%" -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT LIKE "DN_%" -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT IN ("FP_DE", "FP_JP", "BM_VN", "BM_KR") -- Eliminate irrelevant entity_ids in APAC
    AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in MENA
    AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in Europe
    AND p.entity_id NOT IN ("CD_CO") -- Eliminate irrelevant entity_ids in LATAM
;
