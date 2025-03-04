{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['syndrome']},
      {'columns': ['disease']},
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
      {'columns': ['suspected']},
      {'columns': ['tested']},
      {'columns': ['confirmed']},
      {'columns': ['admitted']},
      {'columns': ['discharged']},
      {'columns': ['died']},
      {'columns': ['probable']},
      {'columns': ['contact']},
      {'columns': ['completed']},
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
    'Marburg' AS disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
    name,
    sex,
    age_years,
    age_group,
    nationality,
    id_number,
    occupation,
    occupation_other,
    email_address,
    temperature,
    classification,
    traveller_action,
    traveller_action_other,
    traveller_date_of_checking,
    three_weeks_exposure_title,
    participated_in_patient_care,
    participated_in_burial,
    one_week_exposure_title,
    symptom_fever,
    symptom_diarrhoea,
    generic_headache,
    symptom_joint_muscle_pain,
    symptom_bone_pain,
    symptom_unexplained_bruising,
    symptom_unexplained_body_weakness,
    symptom_internal_external_bleeding,
    symptom_sore_painfull_throat,
    symptom_cough_vomiting,
    symptom_common_cold,
    symptom_weight_loss,
    date_of_arrival,
    point_of_entry,
    point_of_entry_other,
    country_of_departure,
    town_of_departure,
    mode_of_transport,
    registration_number_of_conveyance,
    number_of_seat,
    purpose_of_travel,
    expected_duration_of_stay,
    travelled_to_country_with_confirmed_cases,
    travel_country,
    travel_city_town_village,
    travel_date_of_visit_start,
    travel_date_of_visit_end,
    local_county,
    local_city_town_village,
    local_sublocation_estate,
    local_house_hotel,
    local_postal_address,
    local_phone_number_of_contact,
    local_phone_number_used_while_in_kenya,
    country,
    county,
    subcounty,
    (1)::integer AS suspected,
    null::integer AS tested,
    null::integer AS confirmed,
    null::integer AS admitted,
    null::integer AS discharged,
    null::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_marburg_traveler') }}
