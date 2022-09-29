-- Step 17: Append the output of `final_vendor_list_all_data_temp_loved_brands_scaled_code` to the final table `final_vendor_list_all_data_loved_brands_scaled_code`
INSERT `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
SELECT *
FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_temp_loved_brands_scaled_code`
;
