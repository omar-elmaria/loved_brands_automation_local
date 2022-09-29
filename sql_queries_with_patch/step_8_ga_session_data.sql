-- Step 8: Get session data for all relevant vendors over the specified time frame
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS
SELECT DISTINCT
    x.created_date, -- Date of the ga session
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.platform, -- Operating system (iOS, Android, Web, etc.)
    x.brand, -- Talabat, foodpanda, Foodora, etc.
    x.events_ga_session_id, -- GA session ID
    x.fullvisitor_id, -- The visit_id defined by Google Analytics
    x.visit_id, -- 	The visit_id defined by Google Analytics
    x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
    x.total_transactions, -- The total number of transactions in the GA session
    x.ga_dps_session_id, -- DPS session ID

    x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
    x.sessions.endpoint, -- The endpoint from where the DPS request is coming
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
    x.sessions.experiment_id AS test_id, -- Experiment ID
    CASE
        WHEN x.sessions.vertical_parent IS NULL THEN NULL
        WHEN LOWER(x.sessions.vertical_parent) IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN LOWER(x.sessions.vertical_parent) = "shop" THEN "shop"
        WHEN LOWER(x.sessions.vertical_parent) = "darkstores" THEN "darkstores"
    END AS vertical_parent, -- Parent vertical
    x.sessions.customer_status, -- The customer.tag, indicating whether the customer is new or not
    x.sessions.location, -- The customer.location
    x.sessions.variant_concat, -- The concatenation of all the existing variants for the dps session id. There might be multiple variants due to location changes or session timeout
    x.sessions.location_concat, -- The concatenation of all the existing locations for the dps session id
    x.sessions.customer_status_concat, -- The concatenation of all the existing customer.tag for the dps session id

    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    e.vertical_type, -- This field is NULL for event types home_screen.loaded and shop_list.loaded 
    e.event_time, -- The timestamp of the event's creation
    e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
    e.expedition_type, -- The delivery type of the session, pickup or delivery

    dps.city_id, -- City ID based on the DPS session
    dps.city_name, -- City name based on the DPS session
    dps.id AS zone_id, -- Zone ID based on the DPS session
    dps.name AS zone_name, -- Zone name based on the DPS session
    dps.timezone, -- Time zone of the city based on the DPS session

    ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(dps_zone) AS dps
-- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the relevant zones
INNER JOIN `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code` AS cd
    ON TRUE
        AND x.entity_id = cd.entity_id
        AND x.country_code = cd.country_code
        AND dps.city_name = cd.city_name
        AND ST_CONTAINS(cd.zone_shape, x.ga_location)
WHERE TRUE
    -- Filter for the relevant combinations of entity, country_code, city_name, and zone_name
    AND CONCAT(x.entity_id, " | ", x.country_code, " | ", dps.city_name, " | ", dps.name) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", city_name, " | ", zone_name) AS entity_country_city_zone
        FROM `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code`
    )
    -- Filter for the relevant combinations of entity, country_code, and vendor_code
    AND CONCAT(x.entity_id, " | ", x.country_code, " | ", e.vendor_code) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
        FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
    )
    -- Extract session data over the specified time frame
    AND x.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) -- Sessions' start and end date
    -- Filter for 'shop_details.loaded', 'transaction' events as we only need those to calculate CVR3
    AND e.event_action IN ("shop_details.loaded", "transaction") -- transaction / shop_details.loaded = CVR3
;
