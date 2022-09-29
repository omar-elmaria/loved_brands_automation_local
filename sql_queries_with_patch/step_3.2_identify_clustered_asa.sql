-- Step 3.2: Identify the ASAs that have been clustered
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.parent_child_asa_linking_loved_brands_scaled_code` AS
WITH original_asa AS (
    SELECT DISTINCT
        entity_id,
        country_code,
        asa_name,
        asa_id
    FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
),

parent_and_child_asa AS (
    SELECT
        a.*,
        b.entity_id AS entity_id_linked_asa,
        b.country_code AS country_code_linked_asa,
        b.asa_name AS linked_asa
    FROM original_asa AS a
    CROSS JOIN original_asa AS b
)

SELECT
    a.*,
    -- If child = parent + "_LB" OR parent + "_LB" = child
    CASE WHEN LOWER(a.asa_name) = LOWER(CONCAT(linked_asa, "_LB")) OR LOWER(CONCAT(a.asa_name, "_LB")) = LOWER(linked_asa) THEN TRUE ELSE FALSE END AS is_asa_clustered,
    TRIM(a.asa_name, "_LB") AS asa_common_name
FROM parent_and_child_asa AS a
WHERE TRUE
    AND CASE WHEN LOWER(a.asa_name) = LOWER(CONCAT(linked_asa, "_LB")) OR LOWER(CONCAT(a.asa_name, "_LB")) = LOWER(linked_asa) THEN TRUE ELSE FALSE END = TRUE
    AND a.entity_id = a.entity_id_linked_asa
    AND a.country_code = a.country_code_linked_asa
ORDER BY 1, 2, 3
;
