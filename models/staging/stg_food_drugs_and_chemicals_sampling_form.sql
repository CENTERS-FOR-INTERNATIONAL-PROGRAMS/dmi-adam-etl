SELECT
    (doc ->> '_id')::text AS doc_id,
    (doc ->> '_rev')::text AS doc_rev,
    (doc ->> 'type')::text AS doc_type,
    (doc ->> 'df_complete')::text AS df_complete,
    (doc ->> 'df_submitted')::text AS df_submitted,
    (doc ->> 'df_parent_complete')::text AS df_parent_complete,
    (doc ->> 'event_ident')::text AS event_ident,
    (doc ->> 'mform_event')::text AS mform_event,
    (doc ->> 'parent_ident')::text AS parent_ident,
    (doc -> 'created_scope' ->> 'user_role')::text AS created_role,
    (doc -> 'modified_scope' ->> 'user_role')::text AS modified_role,
    (doc ->> 'mform_id')::text AS mform_id,
    (doc ->> 'fform_id')::text AS form_id,
    (doc ->> 'ident')::text AS case_unique_id,
    (doc ->> 'created_username')::text AS created_username,
    (doc ->> 'created_timestamp')::text AS created_timestamp,
    (doc ->> 'modified_username')::text AS modified_username,
    (doc ->> 'modified_timestamp')::text AS modified_timestamp,
    (doc -> 'location' ->> 'accuracy')::text AS location_accuracy,
    (doc -> 'location' ->> 'latitude')::text AS location_latitude,
    (doc -> 'location' ->> 'longitude')::text AS location_longitude,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'sample_number' ->> 'df_value')::text AS sample_number,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'time_of_sample_collection' ->> 'df_value')::text AS time_of_sample_collection,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'type_of_food_product' ->> 'df_value')::text AS type_of_food_product,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'type_of_food_sample' ->> 'df_value')::text AS type_of_food_sample,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'name_of_product' ->> 'df_value')::text AS name_of_product,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'description_of_product' ->> 'df_value')::text AS description_of_product,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'batch_number' ->> 'df_value')::text AS batch_number,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_manufacture' ->> 'df_value')::text AS date_of_manufacture,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_expiry' ->> 'df_value')::text AS date_of_expiry,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'name_of_manufacturer' ->> 'df_value')::text AS name_of_manufacturer,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'name_of_importer_dealer' ->> 'df_value')::text AS name_of_importer_dealer,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'method_of_collection' ->> 'df_value')::text AS method_of_collection,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'size_of_lot_sample' ->> 'df_value')::text AS size_of_lot_sample,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'specimen_seal_used' ->> 'df_value')::text AS specimen_seal_used,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'reason_for_collection' ->> 'df_value')::text AS reason_for_collection,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'reason_for_collection_other' ->> 'df_value')::text AS reason_for_collection_other,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'mode_of_sample_delivery' ->> 'df_value')::text AS mode_of_sample_delivery,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_dispatch' ->> 'df_value')::text AS date_of_dispatch,
    (doc -> 'DForms' -> 'sample_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'time_of_dispatch' ->> 'df_value')::text AS time_of_dispatch,
    (doc -> 'DForms' -> 'shipping_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'number_of_invoice' ->> 'df_value')::text AS number_of_invoice,
    (doc -> 'DForms' -> 'shipping_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_invoice' ->> 'df_value')::text AS date_of_invoice,
    (doc -> 'DForms' -> 'shipping_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'shipping_record' ->> 'df_value')::text AS shipping_record,
    (doc -> 'DForms' -> 'shipping_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'date_of_shipping' ->> 'df_value')::text AS date_of_shipping,
    (doc -> 'DForms' -> 'collector_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'sample_collector_name' ->> 'df_value')::text AS sample_collector_name,
    (doc -> 'DForms' -> 'collector_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'sample_collector_designation' ->> 'df_value')::text AS sample_collector_designation,
    (doc -> 'DForms' -> 'collector_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'collector_phone_number' ->> 'df_value')::text AS collector_phone_number,
    (doc -> 'DForms' -> 'collector_information_fdc' -> 0 -> 'DFields' -> 'values' -> 'collectors_email_address' ->> 'df_value')::text AS collectors_email_address,
    (doc -> 'DFields' -> 'values' -> 'title_site_information' ->> 'df_value')::text AS title_site_information,
    (doc -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DFields' -> 'values' -> 'type_of_premise' ->> 'df_value')::text AS type_of_premise,
    (doc -> 'DFields' -> 'values' -> 'name_of_premise' ->> 'df_value')::text AS name_of_premise,
    (doc -> 'DFields' -> 'values' -> 'premise_phone_number' ->> 'df_value')::text AS premise_phone_number,
    (doc -> 'DFields' -> 'values' -> 'premise_email_address' ->> 'df_value')::text AS premise_email_address
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = '34cd5e30-376a-11ef-a8a8-45bab7ce775a'
