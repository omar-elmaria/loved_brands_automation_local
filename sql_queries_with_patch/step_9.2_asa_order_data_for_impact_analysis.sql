-- Step 9.2: Get order data for all relevant ASAs over the specified time frame. We will need this information to calculate the potential impact of "Loved Brands"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_order_data_for_impact_analysis_loved_brands_scaled_code` AS
WITH temp_tbl AS (
    SELECT
        o.entity_id,
        o.country_code,
        CAST(o.assignment_id AS INT64) AS asa_id, -- When vendor_price_scheme_type = 'Automatic scheme', assignment_id = asa_id
        o.platform_order_code,
        o.exchange_rate,
        CASE
            WHEN ent.region IN ("Europe", "Asia") THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
                pd.delivery_fee_local,
                IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.delivery_fee_local)
            )
            WHEN ent.region NOT IN ("Europe", "Asia") THEN (CASE WHEN o.is_delivery_fee_covered_by_voucher = FALSE AND o.is_delivery_fee_covered_by_discount = FALSE THEN o.delivery_fee_local ELSE 0 END)
        END AS actual_df_paid_by_customer
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` AS o
    -- The "pd_orders" table contains info on the orders in Pandora countries
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd ON o.entity_id = pd.global_entity_id AND o.platform_order_code = pd.code AND o.created_date = pd.created_date_utc
    INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON o.entity_id = ent.entity_id AND o.country_code = ent.country_code -- Filter only for active DH entities
    WHERE TRUE
        AND CONCAT(o.entity_id, " | ", o.country_code, " | ", o.assignment_id) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", asa_id) AS entity_country_asa
            FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
        )
        AND o.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        AND o.vendor_price_scheme_type = "Automatic scheme" -- Filter for orders coming from ASA schemes
        AND o.delivery_status = "completed" -- Successful order
)

SELECT
    entity_id,
    country_code,
    asa_id,
    COUNT(DISTINCT platform_order_code) AS num_orders_asa,
    ROUND(SUM(actual_df_paid_by_customer), 2) AS total_df_revenue_local,
    ROUND(SUM(actual_df_paid_by_customer / exchange_rate), 2) AS total_df_revenue_eur,
    ROUND(SUM(actual_df_paid_by_customer) / COUNT(DISTINCT platform_order_code), 2) AS avg_df_asa_local,
    ROUND(SUM(actual_df_paid_by_customer / exchange_rate) / COUNT(DISTINCT platform_order_code), 2) AS avg_df_asa_eur
FROM temp_tbl
GROUP BY 1, 2, 3
;
