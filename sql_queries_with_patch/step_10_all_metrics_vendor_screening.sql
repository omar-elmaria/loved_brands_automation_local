-- Step 10: Compute the total orders, visits, and CVR3 for each vendor code in `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code` AS
WITH all_metrics AS (
    SELECT
        v.*,
        COALESCE(o.num_orders, 0) AS num_orders,
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0) AS num_unique_vendor_visits, -- If a vendor was visited more than once in the same session, it's one visit
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN event_time END), 0) AS num_total_vendor_impressions, -- If a vendor was visited more than once in the same session, count all impressions
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END), 0) AS num_transactions,
        -- We choose to round to 5 decimal places because we compare the CVRs to one another at a later point in the code when we calculate the pct drop in CVR from the base
        -- More precision is needed to avoid calculation errors due to rounding
        COALESCE(
            ROUND(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END) / NULLIF(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0), 5),
            0
        ) AS cvr3
    FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` AS v
    LEFT JOIN `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS s
        ON TRUE
            AND v.entity_id = s.entity_id
            AND v.country_code = s.country_code
            AND v.vendor_code = s.vendor_code -- LEFT JOIN because we assume that the vendors table contains ALL vendors with and without sessions, so any vendors without sessions will get a "zero"
    LEFT JOIN `dh-logistics-product-ops.pricing.order_data_for_vendor_screening_loved_brands_scaled_code` AS o
        ON TRUE
            AND v.entity_id = o.entity_id
            AND v.country_code = o.country_code
            AND v.vendor_code = o.vendor_code -- LEFT JOIN for the same reason in the statement above
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

pct_ranks AS (
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY num_orders), 4) AS orders_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY num_unique_vendor_visits), 4) AS unique_visits_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY cvr3), 4) AS cvr3_pct_rank
    FROM all_metrics
),

additional_fields AS (
    SELECT
        *,
        -- Entity orders and ASA order share
        SUM(num_orders) OVER (PARTITION BY entity_id, country_code) AS entity_orders,
        SUM(num_orders) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders,
        ROUND(SUM(num_orders) OVER (PARTITION BY entity_id, country_code, asa_id) / NULLIF(SUM(num_orders) OVER (PARTITION BY entity_id, country_code), 0), 4) AS asa_order_share_of_entity,

        -- ASA order count after every filtering step
        SUM(CASE WHEN unique_visits_pct_rank >= sessions_ntile_thr THEN num_orders END) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_visits_filter,
        SUM(
            CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                THEN num_orders END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_visits_and_orders_filters,

        SUM(
            CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                AND cvr3_pct_rank >= cvr3_ntile_thr
                THEN num_orders END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_all_initial_filters,

        -- ASA vendor count after every filtering step
        COUNT(DISTINCT CASE WHEN unique_visits_pct_rank >= sessions_ntile_thr THEN vendor_code END) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_visits_filter,

        COUNT(
            DISTINCT CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                THEN vendor_code END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_visits_and_orders_filters,

        COUNT(
            DISTINCT CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                AND cvr3_pct_rank >= cvr3_ntile_thr
                THEN vendor_code END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_all_initial_filters
    FROM pct_ranks
)

SELECT
    *,

    -- ASA order share after every filtering step
    ROUND(asa_orders_after_visits_filter / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_visits_filter,
    ROUND(asa_orders_after_visits_and_orders_filters / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_visits_and_orders_filters,
    ROUND(asa_orders_after_all_initial_filters / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_all_initial_filters,

    -- Percentage of vendors remaining in the ASA after every filtering step
    ROUND(vendor_count_remaining_after_visits_filter / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_visits_filter,
    ROUND(vendor_count_remaining_after_visits_and_orders_filters / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_visits_and_orders_filters,
    ROUND(vendor_count_remaining_after_all_initial_filters / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_all_initial_filters
FROM additional_fields
;
