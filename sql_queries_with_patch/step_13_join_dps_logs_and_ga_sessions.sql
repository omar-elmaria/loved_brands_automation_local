-- Step 13: Join the DPS logs to the GA sessions data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_dps_sessions_loved_brands_scaled_code` AS
SELECT
    x.*,
    logs.df_total,
    logs.scheme_id,
    logs.vendor_price_scheme_type,
    logs.created_at AS dps_logs_created_at
FROM `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS x
-- With this join, ~ 12.9% of all rows get a NULL DF_total as a result of filtering out multipleFee endpoint requests
LEFT JOIN `dh-logistics-product-ops.pricing.dps_logs_loved_brands_scaled_code` AS logs -- You can use an INNER JOIN here if it's important to have a DF value associated with every session
    ON TRUE
        AND x.entity_id = logs.entity_id
        AND x.country_code = logs.country_code
        AND x.ga_dps_session_id = logs.dps_session_id
        -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
        AND x.vendor_code = logs.vendor_code
ORDER BY events_ga_session_id, event_time
;
