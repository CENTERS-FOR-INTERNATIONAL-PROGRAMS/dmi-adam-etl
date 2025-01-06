SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    name_of_traveller || ' (' || classification_of_traveller || ')' AS contact_tree_label,
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
    'Marburg'::text AS disease,
    CASE WHEN traveller_date_of_checking ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(traveller_date_of_checking, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN traveller_date_of_checking ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(traveller_date_of_checking, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
    null::text AS epid,
    traveller_date_of_checking::text AS date_of_investigation,
    classification_of_traveller::text AS type_of_case,
    name_of_traveller::text AS name,
    sex_of_traveller::text AS sex,
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
    nationality_of_traveller::text AS nationality,
    id_number::text AS id_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    email_address::text AS email_address,
    temperature_of_traveller::text AS temperature,
    classification_of_traveller::text AS classification,
    traveller_action::text AS traveller_action,
    traveller_action_other::text AS traveller_action_other,
    traveller_date_of_checking::text AS traveller_date_of_checking,
    three_weeks_exposure_title::text AS three_weeks_exposure_title,
    participated_in_patient_care::text AS participated_in_patient_care,
    participated_in_burial::text AS participated_in_burial,
    one_week_exposure_title::text AS one_week_exposure_title,
    symptom_fever::text AS symptom_fever,
    symptom_diarrhoea::text AS symptom_diarrhoea,
    generic_headache::text AS generic_headache,
    symptom_joint_muscle_pain::text AS symptom_joint_muscle_pain,
    symptom_bone_pain::text AS symptom_bone_pain,
    symptom_unexplained_bruising::text AS symptom_unexplained_bruising,
    symptom_unexplained_body_weakness::text AS symptom_unexplained_body_weakness,
    symptom_internal_external_bleeding::text AS symptom_internal_external_bleeding,
    symptom_sore_painfull_throat::text AS symptom_sore_painfull_throat,
    symptom_cough_vomiting::text AS symptom_cough_vomiting,
    symptom_common_cold::text AS symptom_common_cold,
    symptom_weight_loss::text AS symptom_weight_loss,
    date_of_arrival::text AS date_of_arrival,
    point_of_entry::text AS point_of_entry,
    point_of_entry_other::text AS point_of_entry_other,
    country_of_departure::text AS country_of_departure,
    town_of_departure::text AS town_of_departure,
    mode_of_transport::text AS mode_of_transport,
    registration_number_of_conveyance::text AS registration_number_of_conveyance,
    number_of_seat::text AS number_of_seat,
    purpose_of_travel::text AS purpose_of_travel,
    expected_duration_of_stay::text AS expected_duration_of_stay,
    travelled_to_country_with_confirmed_cases::text AS travelled_to_country_with_confirmed_cases,
    travel_country::text AS travel_country,
    travel_city_town_village::text AS travel_city_town_village,
    travel_date_of_visit_start::text AS travel_date_of_visit_start,
    travel_date_of_visit_end::text AS travel_date_of_visit_end,
    local_county::text AS local_county,
    local_city_town_village::text AS local_city_town_village,
    local_sublocation_estate::text AS local_sublocation_estate,
    local_house_hotel::text AS local_house_hotel,
    local_postal_address::text AS local_postal_address,
    local_phone_number_of_contact::text AS local_phone_number_of_contact,
    local_phone_number_used_while_in_kenya::text AS local_phone_number_used_while_in_kenya,
    travel_country::text AS country,
    local_county::text AS county,
    local_sublocation_estate::text AS subcounty
FROM {{ ref('stg_marburg_traveler') }}
