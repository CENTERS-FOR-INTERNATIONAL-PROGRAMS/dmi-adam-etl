SELECT
    (doc ->> '_id')::text AS doc_id,
    (doc ->> '_rev')::text AS doc_rev,
    (doc ->> 'type')::text AS doc_type,
    (doc ->> 'df_complete')::text AS doc_df_complete,
    (doc ->> 'df_submitted')::text AS doc_df_submitted,
    (doc ->> 'df_parent_complete')::text AS doc_df_parent_complete,
    (doc ->> 'event_ident')::text AS doc_event_ident,
    (doc ->> 'mform_event')::text AS doc_mform_event,
    (doc ->> 'parent_ident')::text AS doc_parent_ident,
    (doc -> 'created_scope' ->> 'user_role')::text AS created_scope_user_role,
    (doc -> 'modified_scope' ->> 'user_role')::text AS modified_scope_user_role,
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
    (doc -> 'DFields' -> 'values' -> 'name_of_traveler' ->> 'df_value')::text AS name_of_traveler,
    (doc -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DFields' -> 'values' -> 'sex_of_traveler' ->> 'df_value')::text AS sex_of_traveler,
    (doc -> 'DFields' -> 'values' -> 'nationality_of_traveler' ->> 'df_value')::text AS nationality_of_traveler,
    (doc -> 'DFields' -> 'values' -> 'id_number' ->> 'df_value')::text AS id_number,
    (doc -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DFields' -> 'values' -> 'email_address' ->> 'df_value')::text AS email_address,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'day_of_follow_up' ->> 'df_value')::text AS day_of_follow_up,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'date_of_follow_up' ->> 'df_value')::text AS date_of_follow_up,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'contact_is_symptomatic' ->> 'df_value')::text AS contact_is_symptomatic
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = '59635360-67c3-11ef-8f3c-c9e80a669bbc'
AND
    (doc ->> 'ident') IS NOT NULL
