-- Step 16: Pull all the data associated with the "Loved Brands" that were obtained in the previous step
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.final_vendor_list_all_data_temp_loved_brands_scaled_code` AS
WITH temp_tbl AS (
    SELECT
        -- Grouping variables
        region,
        entity_id,
        country_code,
        asa_id,
        asa_name,
        vendor_count_caught_by_asa,
        vendor_code,
        -- Vendor data (DFs, CVR per DF tier, and percentage changes from the base tier to each subsequent one)
        CASE WHEN num_yes > 0 THEN 'Y' ELSE 'N' END AS is_LB,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(DF_total AS STRING) ORDER BY DF_total), ', ') AS DFs_seen_in_sessions,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(is_LB_test_passed AS STRING) ORDER BY DF_total), ', ') AS is_LB_test_passed,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(CVR3 AS STRING) ORDER BY DF_total), ', ') AS vendor_cvr3_by_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(pct_chng_of_actual_cvr3_from_base AS STRING) ORDER BY DF_total), ', ') AS pct_chng_of_actual_cvr3_from_base,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(asa_cvr3_per_df AS STRING) ORDER BY DF_total), ', ') AS asa_cvr3_per_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(pct_chng_of_asa_cvr3_from_base AS STRING) ORDER BY DF_total), ', ') AS pct_chng_of_asa_cvr3_from_base,
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_plus_cvr_thresholds_loved_brands_scaled_code`
    GROUP BY 1,2,3,4,5,6,7,8
)

SELECT 
    -- Identifiers
    a.region,
    a.entity_id,
    a.country_code,
    a.asa_id,
    a.asa_name,
    a.vendor_code,
    c.vertical_type,
    CASE WHEN b.is_LB IS NOT NULL THEN 'Top 25%' ELSE 'Bottom 75%' END AS vendor_rank,
    
    -- Vendor data (DFs, CVR per DF tier, and percentage changes from the base tier to each subsequent one) 
    COALESCE(b.is_LB, 'N') AS is_LB, -- Impute the label of the vendor. If the vendor is in the bottom 75%, it's a non-LB by definition
    b.DFs_seen_in_sessions,
    COALESCE(b.is_LB_test_passed, 'N') is_LB_test_passed, -- If the vendor is in the bottom 75% of vendors, the LB test is not passed by definition
    b.vendor_cvr3_by_df,
    b.pct_chng_of_actual_cvr3_from_base,
    b.asa_cvr3_per_df,
    b.pct_chng_of_asa_cvr3_from_base,
    
    -- Vendor data (Busines metrics and other KPIs)
    a.num_orders,
    a.num_unique_vendor_visits,
    a.cvr3,
    a.orders_pct_rank,
    a.unique_visits_pct_rank,
    a.CVR3_pct_rank,

    -- ASA and entity data
    a.entity_orders,
    a.asa_orders,
    a.asa_order_share_of_entity,
    a.asa_orders_after_visits_filter,
    a.asa_orders_after_visits_and_orders_filters,		
    a.asa_orders_after_all_initial_filters,
    SUM(CASE WHEN b.is_LB = 'Y' THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, b.asa_id) AS asa_orders_after_LB_logic,
    a.asa_order_share_after_visits_filter,		
    a.asa_order_share_after_visits_and_orders_filters,
    a.asa_order_share_after_all_initial_filters,
    ROUND(SUM(CASE WHEN b.is_LB = 'Y' THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, b.asa_id) / NULLIF(a.entity_orders, 0), 4) AS asa_order_share_after_LB_logic,		
    
    -- Vendor count and share after each filtering stage
    a.vendor_count_caught_by_asa,
    a.vendor_count_remaining_after_visits_filter,		
    a.vendor_count_remaining_after_visits_and_orders_filters,		
    a.vendor_count_remaining_after_all_initial_filters,
    COUNT(DISTINCT CASE WHEN b.is_LB = 'Y' THEN b.vendor_code ELSE NULL END) OVER (PARTITION BY b.entity_id, b.country_code, b.asa_id) AS vendor_count_remaining_after_LB_logic,
    a.perc_vendors_remaining_after_visits_filter,
    a.perc_vendors_remaining_after_visits_and_orders_filters,
    a.perc_vendors_remaining_after_all_initial_filters,
    ROUND(COUNT(DISTINCT CASE WHEN b.is_LB = 'Y' THEN b.vendor_code ELSE NULL END) OVER (PARTITION BY b.entity_id, b.country_code, b.asa_id) / NULLIF(a.vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_LB_logic,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code` a -- Contains ALL vendors (bottom 75% and top 25%)
LEFT JOIN temp_tbl b
    ON TRUE
        AND a.entity_id = b.entity_id 
        AND a.country_code = b.country_code 
        AND a.asa_id = b.asa_id
        AND a.vendor_code = b.vendor_code
LEFT JOIN `dh-logistics-product-ops.curated_data_shared_central_dwh.vendors` c ON a.entity_id = c.global_entity_id AND a.vendor_code = c.vendor_id
ORDER BY 1,2,3,4,5,6,7;