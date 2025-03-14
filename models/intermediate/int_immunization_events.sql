SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    date_of_event_start || ' (' || description_of_event || ')' AS contact_tree_label,
    mform_id::text AS mform_id,
    mform_event::text AS mform_event,
    event_ident::text AS event_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    (CASE WHEN df_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS completed,
    (CASE WHEN df_parent_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS parent_completed,
    created_role::text AS created_role,
    modified_role::text AS modified_role,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    ''::text AS syndrome,
    ''::text AS disease,
    CASE WHEN date_of_event_start ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_event_start, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_event_start ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_event_start, 'DD/MM/YYYY'), 'YYYY"-"IW') ELSE NULL END AS epi_week,
    county::text AS county,
    ''::text AS subcounty,
    date_of_event_start::text AS date_of_event_start,
    date_of_event_end::text AS date_of_event_end,
    description_of_event::text AS description_of_event
FROM {{ ref('stg_immunization_events') }}
WHERE df_complete::text = 'true'
