-- Step 12: Get data about the DF seen in the session from the DPS logs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_logs_loved_brands_scaled_code` AS
WITH dps_logs_stg_1 AS (
    SELECT
        logs.entity_id,
        LOWER(logs.country_code) AS country_code,
        logs.created_date,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        vendors,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
    FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` AS logs
    INNER JOIN `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code` AS cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) -- Filter for sessions in the zones specified above
    WHERE TRUE
        -- Filter for the relevant combinations of entity and country_code
        AND CONCAT(logs.entity_id, " | ", LOWER(logs.country_code)) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code) AS entity_country
            FROM `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code`
        )
        -- Do NOT filter for multiplFee (MF) endpoints because the query times out if you do so. singleFee endpoint requests are sufficient for our purposes even though we lose a bit of data richness when we don't consider MF requests
        -- We lose data on the delivery fee seen in sessions for about 7.5% of all GA sessions in "session_data_for_vendor_screening_loved_brands_scaled_code" by adding this filter. However, we gain much more in code efficiency   
        AND endpoint = "singleFee"
        -- Do NOT use the "start_date" and "end_date" params here. This will inc. the query size
        AND logs.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
),

dps_logs_stg_2 AS (
    SELECT DISTINCT
        dps.* EXCEPT(session_timestamp, created_at, vendors),
        v.id AS vendor_code,
        v.meta_data.scheme_id,
        v.meta_data.vendor_price_scheme_type,
        v.delivery_fee.total AS df_total,
        dps.session_timestamp,
        dps.created_at
    FROM dps_logs_stg_1 AS dps
    LEFT JOIN UNNEST(vendors) AS v
    WHERE TRUE
        -- Filter for the relevant combinations of entity, country_code, and vendor_code
        AND CONCAT(dps.entity_id, " | ", dps.country_code, " | ", v.id) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
            FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
        )

-- Don't filter for the relevant combinations of entity, country_code, and scheme_id even though the code takes longer to run
-- We care about sessions that were associated with the DF amounts of the respective ASA, not which scheme these DF amounts came from. To a customer, the DF amount is what matters. They don't know which scheme it came from
)

SELECT *
FROM dps_logs_stg_2
-- Create a row counter to take the last delivery fee seen in the session for each vendor_id. We assume that this is the one that the customer took their decision to purchase/not purchase on
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, dps_session_id, vendor_code ORDER BY created_at DESC) = 1
ORDER BY dps_session_id, vendor_code, created_at
;
