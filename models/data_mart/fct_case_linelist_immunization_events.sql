{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['completed']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
    ]
)}}

SELECT
    id,
    parent_id,
    contact_tree_label,
    mform_id,
    mform_event,
    event_id,
    form_id,
    case_unique_id,
    completed,
    parent_completed,
    created_role,
    modified_role,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    location_accuracy,
    location_latitude,
    location_longitude,
    syndrome,
    disease,
    case_date,
    epi_week,
    county,
    subcounty,
    date_of_event_start,
    date_of_event_end,
    description_of_event
FROM {{ ref('int_immunization_events') }}
