SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    name_of_traveler || ' (' || nationality_of_traveler || ')' AS contact_tree_label,
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
    'Mpox Traveller'::text AS disease,
    CASE WHEN date_of_follow_up ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_follow_up, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_follow_up ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_follow_up, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
    name_of_traveler::text AS name,
     CASE 
        WHEN age_years ~ '^[0-9]+(\.[0-9]+)?$' THEN age_years::float
        ELSE NULL
    END AS age_years,
    CASE
        WHEN NOT (age_years ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN age_years::float < 2 THEN '0-2 yrs'
        WHEN age_years::float BETWEEN 2 AND 5 THEN '2-5 yrs'
        WHEN age_years::float BETWEEN 5 AND 16 THEN '5-16 yrs'
        WHEN age_years::float > 16 THEN '16+ yrs'
        ELSE 'Unknown'
    END AS age_group,
    sex_of_traveler::text AS sex,
    nationality_of_traveler::text AS nationality,
    id_number::text AS id_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    email_address::text AS email_address,
    day_of_follow_up::text AS day_of_follow_up,
    date_of_follow_up::text AS date_of_follow_up,
    contact_is_symptomatic::text AS contact_is_symptomatic
FROM {{ ref('stg_mpox_traveler') }}
WHERE df_complete::text = 'true'
