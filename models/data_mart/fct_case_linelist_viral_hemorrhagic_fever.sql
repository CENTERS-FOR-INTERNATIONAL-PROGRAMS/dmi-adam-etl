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
    'VHF' AS syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    location_of_investigation,
    location_of_investigation_other,
    CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    date_of_birth,
    country,
    county,
    subcounty,
    ward,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    marital_status,
    case_is_pregnant,
    case_has_insurance_cover,
    level_of_education,
    type_of_residence,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_phone_number,
    respondent_relationship,
    respondent_relationship_other,
    respondent_address,
    contact_surname,
    contact_given_name,
    contact_sex,
    contact_age,
    outcome_final_case_classification,
    outcome_final_patient_status,
    outcome_final_patient_status_other,
    outcome_reason_for_referral,
    outcome_date_of_outcome,
    record_exposure,
    exposure_type,
    type_of_exposure,
    food_name,
    food_consumption_date,
    food_source,
    food_participants_count,
    food_affected_participants_count,
    water_sources,
    water_sources_other,
    toilet_types,
    toilet_types_other,
    travel_origin_country,
    travel_origin_city,
    travel_departure_date,
    travel_destination_country,
    travel_destination_city,
    travel_arrival_date,
    animal_exposure,
    animal_exposure_other,
    animal_species,
    animal_species_other,
    animal_exposure_location,
    animal_exposure_date,
    type_of_case_contact,
    type_of_direct_contact,
    type_of_direct_contact_other,
    case_was_symptomatic_during_contact,
    date_of_contact,
    frequency_of_contact,
    duration_of_contact,
    location_of_contact,
    relationship,
    relationship_other,
    title_ppe,
    case_was_wearing_ppe,
    ppe_items_worn,
    ppe_breach,
    hand_hygiene_before_wearing_ppe,
    hand_hygiene_after_wearing_ppe,
    description_of_event,
    location_of_event,
    date_of_event,
    case_was_ill,
    description_of_illness,
    case_seen_at_facility,
    date_first_seen_at_facility,
    admitted as case_admitted,
    health_facility_name,
    admission_date,
    inpatient_number,
    discharge_date,
    patient_status,
    laboratory_sample_collected,
    laboratory_samples_collected_other,
    date_of_sample_collection,
    laboratory_samples_collected,
    laboratory_samples_collected_others,
    sample_date,
    rdt_done,
    rdt_results,
    samples_sent_to_laboratory,
    laboratory_name,
    sample_sent_to_lab_date,
    symptoms,
    symptoms_other,
    symptoms_unexplained_bleeding,
    symptoms_start_date,
    date_of_onset,
    title_symptoms_start_location,
    symptoms_start_county,
    symptoms_start_subcounty,
    symptoms_start_city_town_village_camp,
    outcome,
    title_clinical_care,
    type_of_care,
    name_of_health_facility,
    date_of_admission,
    ward_of_admission,
    outpatient_number,
    outcome_of_case,
    status_of_patient,
    date_of_discharge,
    date_of_death,
    place_of_death,
    place_of_burial,
    laboratory_tests,
    title_laboratory_facility,
    type_of_laboratory_facility,
    name_of_laboratory_facility,
    name_of_laboratory_facility_other,
    date_of_laboratory_testing_results,
    vaccination_ebola_vaccine_received,
    vaccination_ebola_name_of_vaccine,
    vaccination_ebola_number_of_doses,
    vaccination_ebola_date_of_vaccination,
    contact_follow_up_day_of_follow_up,
    contact_follow_up_date_of_follow_up,
    contact_follow_up_contact_is_symptomatic,
    (CASE WHEN type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
    (CASE WHEN rdt_done = 'Yes' THEN 1 WHEN laboratory_sample_collected = 'Yes' THEN 1 WHEN laboratory_tests IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN rdt_results = 'Positive' THEN 1 WHEN outcome_final_case_classification = 'Confirmed' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN admitted = 'Yes' THEN 1 WHEN outcome = 'Admitted' THEN 1 WHEN admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN patient_status = 'Discharged' THEN 1 WHEN discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN patient_status = 'Died' THEN 1 WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_viral_hemorrhagic_fever') }}
