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
    (doc -> 'DFields' -> 'values' -> 'disease' ->> 'df_value')::text AS disease,
    (doc -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DFields' -> 'values' -> 'event_description' ->> 'df_value')::text AS event_description,
    (doc -> 'DFields' -> 'values' -> 'mode_of_transmission' ->> 'df_value')::text AS mode_of_transmission,
    (doc -> 'DFields' -> 'values' -> 'method_of_detection' ->> 'df_value')::text AS method_of_detection,
    (doc -> 'DFields' -> 'values' -> 'date_of_emergence' ->> 'df_value')::text AS date_of_emergence,
    (doc -> 'DFields' -> 'values' -> 'date_of_detection' ->> 'df_value')::text AS date_of_detection,
    (doc -> 'DFields' -> 'values' -> 'date_of_notification' ->> 'df_value')::text AS date_of_notification
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = 'a6c2af70-7f51-11ef-8f38-d1e08b85d1f9'