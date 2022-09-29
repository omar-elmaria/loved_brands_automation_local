-- Step 15.1: Calculate the **overall** CVR3 per DF tier/bucket (i.e., on the ASA level including all the schemes and DF tiers under it). This will be the average performance that we will compare each vendor against
-- The average performance is defined by pooling together the top 25% of vendors in terms of visits, orders, and CVR3. The bottom 75% of vendors are NOT taken into account.
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS
SELECT
    ven.region,
    ven.entity_id,
    ven.country_code,
    ven.master_asa_id,
    ven.asa_common_name,
    ven.vendor_count_caught_by_asa,
    ses.df_total,
    -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0) AS num_unique_vendor_visits,
    -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.event_time END), 0) AS num_transactions,
    COALESCE(
        ROUND(
            COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.event_time END)
            / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0),
            5
        ),
        0
    ) AS asa_cvr3_per_df
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
    ) -- Filter for the main DF thresholds under each ASA (RIGHT NOW as reported by dps_config_versions) because the DF tiers that were obtained from the logs could contain ones that are not related to the ASA
GROUP BY 1, 2, 3, 4, 5, 6, 7
;
