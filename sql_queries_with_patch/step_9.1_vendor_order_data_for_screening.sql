-- Step 9.1: Get order data for all relevant vendors over the specified time frame
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.order_data_for_vendor_screening_loved_brands_scaled_code` AS
SELECT
    entity_id,
    LOWER(country_code) AS country_code,
    vendor_id AS vendor_code,
    COUNT(DISTINCT platform_order_code) AS num_orders
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
WHERE TRUE
    AND CONCAT(entity_id, " | ", country_code, " | ", vendor_id) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
        FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
    )
    AND created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
    AND delivery_status = "completed" -- Successful order
GROUP BY 1, 2, 3
;
