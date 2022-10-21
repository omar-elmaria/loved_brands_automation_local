-- Step 14: Now, we are ready to apply the Loved Brands logic
-- Instead of specifying CVR3 drop thresholds that cannot be exceeded in a manual manner, we will calculate the overall CVR3 per DF bucket for each ASA \
-- and use the percentage changes from the lowest DF tier to each subsequent one as our thresholds

-- Step 14.1: Calculate the conversion rate per DF bucket for each vendor
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS
SELECT
    ven.region,
    ven.entity_id,
    ven.country_code,
    ven.master_asa_id,
    ven.asa_common_name,
    ven.vendor_count_caught_by_asa,
    ven.vendor_code,
    ses.df_total,
    -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0) AS num_unique_vendor_visits,
    -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.event_time END), 0) AS num_total_vendor_impressions,
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END), 0) AS num_transactions,
    COALESCE(
        ROUND(
            COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END)
            / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0),
            5
        ),
        0
    ) AS cvr3
FROM `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` AS ven
LEFT JOIN `dh-logistics-product-ops.pricing.ga_dps_sessions_loved_brands_scaled_code` AS ses
    ON TRUE
        AND ven.entity_id = ses.entity_id
        AND ven.country_code = ses.country_code
        AND ven.vendor_code = ses.vendor_code
WHERE TRUE
    AND ses.df_total IS NOT NULL -- Remove DPS sessions that do not return a DF value because any such record would be meaningless
    AND CONCAT(ven.entity_id, " | ", ven.country_code, " | ", ven.master_asa_id, " | ", ses.df_total) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", master_asa_id, " | ", fee) AS entity_country_asa_fee
        FROM `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code`
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 -- Filter for the main DF thresholds under each ASA (RIGHT NOW as reported by dps_config_versions) because the DF tiers that were obtained from the logs could contain ones that are not related to the ASA
;
