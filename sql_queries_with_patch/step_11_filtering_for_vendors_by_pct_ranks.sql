-- Step 11: Filtering for vendors based on percentile ranks
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` AS
SELECT
    -- Identifiers
    region,
    entity_id,
    country_code,
    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    vendor_code,

    -- Orders, visits, impressions, transactions, and CVR3 per vendor
    num_orders,
    num_unique_vendor_visits,
    num_total_vendor_impressions,
    num_transactions,
    cvr3,

    -- Percentile ranks. The comparison set is all vendors within an ASA
    orders_pct_rank,
    unique_visits_pct_rank,
    cvr3_pct_rank,

    -- Order counts after each filtering step
    entity_orders,
    asa_order_share_of_entity,
    asa_orders_after_visits_filter,
    asa_orders_after_visits_and_orders_filters,
    asa_orders_after_all_initial_filters,

    -- Order shares after each filtering step
    asa_order_share_after_visits_filter,
    asa_order_share_after_visits_and_orders_filters,
    asa_order_share_after_all_initial_filters,

    -- Vendor counts remaining after each filtering step
    vendor_count_caught_by_asa,
    vendor_count_remaining_after_visits_filter,
    vendor_count_remaining_after_visits_and_orders_filters,
    vendor_count_remaining_after_all_initial_filters,

    -- Percentage of ASA vendors remaining after each filtering step
    perc_vendors_remaining_after_visits_filter,
    perc_vendors_remaining_after_visits_and_orders_filters,
    perc_vendors_remaining_after_all_initial_filters

FROM `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code`
WHERE unique_visits_pct_rank >= sessions_ntile_thr AND orders_pct_rank >= orders_ntile_thr AND cvr3_pct_rank >= cvr3_ntile_thr
ORDER BY region ASC, entity_id ASC, country_code ASC, master_asa_id ASC, cvr3_pct_rank DESC
;
