-- Step 6: Get data about the DF tiers of each price scheme. We need this table for step 7
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code` AS
SELECT *
FROM (
    SELECT
        a.region,
        a.entity_id,
        a.country_code,
        -- Keep in mind that one scheme could be included in more than one ASA, so joining "dps_config_versions" on the "scheme_ids_per_asa_loved_brands_scaled_code" will produce duplicates and this is expected
        b.asa_id,
        b.master_asa_id,
        b.asa_name,
        b.asa_common_name,
        a.* EXCEPT (region, entity_id, country_code),
        RANK() OVER (PARTITION BY a.entity_id, a.country_code, a.scheme_id, a.travel_time_config_id ORDER BY a.tt_threshold) AS tier
    FROM (
        SELECT DISTINCT
            ps.region,
            ps.entity_id,
            ps.country_code,
            ps.scheme_id,
            h.scheme_name,
            TIMESTAMP_TRUNC(h.active_from, SECOND) AS scheme_active_from,
            TIMESTAMP_TRUNC(h.active_to, SECOND) AS scheme_active_to,
            h.travel_time_config_id,
            ttc.config_name,
            TIMESTAMP_TRUNC(ttc.active_from, SECOND) AS tt_config_active_from,
            TIMESTAMP_TRUNC(ttc.active_to, SECOND) AS tt_config_active_to,
            COALESCE(ttd.travel_time_threshold, 9999999) AS tt_threshold,
            CASE
                WHEN ttd.travel_time_threshold IS NULL THEN 9999999
                ELSE ROUND(FLOOR(ttd.travel_time_threshold) + (ttd.travel_time_threshold - FLOOR(ttd.travel_time_threshold)) * 60 / 100, 2)
            END AS threshold_in_min_and_sec,
            ttd.travel_time_fee AS fee,
            TIMESTAMP_TRUNC(ttd.active_from, SECOND) AS tt_detail_active_from,
            TIMESTAMP_TRUNC(ttd.active_to, SECOND) AS tt_detail_active_to
        FROM `fulfillment-dwh-production.cl.dps_config_versions` AS ps
        LEFT JOIN UNNEST(price_scheme_history) AS h
        LEFT JOIN UNNEST(travel_time_history) AS tth
        LEFT JOIN UNNEST(travel_time_config) AS ttc
        LEFT JOIN UNNEST(travel_time_detail) AS ttd
        WHERE TRUE
            AND h.active_to IS NULL
            AND ttc.active_to IS NULL
            AND ttd.active_to IS NULL
            AND CONCAT(ps.entity_id, " | ", ps.country_code, " | ", ps.scheme_id) IN (
                SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", scheme_id) AS entity_country_scheme
                FROM `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code`
            )
        QUALIFY
            TIMESTAMP_TRUNC(h.active_from, SECOND) = MAX(TIMESTAMP_TRUNC(h.active_from, SECOND)) OVER (PARTITION BY ps.entity_id, ps.country_code, ps.scheme_id)
            AND TIMESTAMP_TRUNC(ttc.active_from, SECOND) = MAX(TIMESTAMP_TRUNC(ttc.active_from, SECOND)) OVER (PARTITION BY ps.entity_id, ps.country_code, ps.scheme_id)
    -- Don't filter for the latest travel time detail record(s) via another QUALIFY statement because this occasionally removes relevant TT tiers
    ) AS a
    LEFT JOIN `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code` AS b
        ON TRUE
            AND a.entity_id = b.entity_id
            AND a.country_code = b.country_code
            AND a.scheme_id = b.scheme_id
)
ORDER BY region, entity_id, country_code, asa_id, scheme_id, tier
;
