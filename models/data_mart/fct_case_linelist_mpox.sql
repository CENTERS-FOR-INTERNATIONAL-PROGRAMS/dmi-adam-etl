{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['completed']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
    ]
)}}

SELECT
    'a52f1b40-89b1-22df-a632-9dcf84b7c051'::text AS id,
    ''::text AS parent_id,
    'Mpox'::text AS contact_tree_label,
    ''::text AS mform_id,
    ''::text AS mform_event,
    ''::text AS event_id,
    ''::text AS form_id,
    ''::text AS case_unique_id,
    'Draft'::text AS completed,
    ''::text AS parent_completed,
    ''::text AS created_role,
    ''::text AS modified_role,
    ''::text AS created_username,
    ''::text AS created_timestamp,
    ''::text AS modified_username,
    ''::text AS modified_timestamp,
    ''::text AS location_accuracy,
    ''::text AS location_latitude,
    ''::text AS location_longitude,
    ''::text AS syndrome,
    'Monkey Pox'::text AS disease,
    null::date AS case_date,
    ''::text AS epi_week,
    ''::text AS epid,
    ''::text AS date_of_investigation,
    ''::text AS location_of_investigation,
    ''::text AS location_of_investigation_other,
    ''::text AS type_of_case,
    ''::text AS family_name,
    ''::text AS given_name,
    ''::text AS sex,
    null::double precision AS age_years,
    null::double precision AS age_months,
    null::double precision AS age_days,
    ''::text AS age_group,
    ''::text AS date_of_birth,
    ''::text AS pregnant,
    ''::text AS phone_number,
    ''::text AS occupation,
    ''::text AS occupation_other,
    ''::text AS title_residence,
    ''::text AS type_of_residence,
    ''::text AS country,
    ''::text AS county,
    ''::text AS subcounty,
    ''::text AS ward,
    ''::text AS town_village_camp,
    ''::text AS landmark,
    ''::text AS marital_status,
    ''::text AS level_of_education,
    ''::text AS case_has_insurance_cover,
    ''::text AS title_parent_guradian,
    ''::text AS guardian_family_name,
    ''::text AS guardian_given_name,
    ''::text AS guardian_phone_number,
    ''::text AS title_respondent,
    ''::text AS respondent,
    ''::text AS respondent_family_name,
    ''::text AS respondent_given_name,
    ''::text AS respondent_address,
    ''::text AS respondent_phone_number,
    ''::text AS respondent_relationship,
    ''::text AS symptoms,
    ''::text AS symptoms_other,
    ''::text AS fever_temperature,
    ''::text AS locations_of_lymphadenopathy,
    ''::text AS date_of_onset,
    ''::text AS start_location_of_rash,
    ''::text AS start_location_of_rash_other,
    ''::text AS spread_location_of_rash,
    ''::text AS spread_location_of_rash_other,
    ''::text AS number_of_lesions,
    ''::text AS development_of_rash,
    ''::text AS distribution_of_rash,
    ''::text AS history_of_immunosuppressive_conditions,
    ''::text AS hiv_positive,
    ''::text AS date_of_hiv_diagnosis,
    ''::text AS patient_on_art,
    ''::text AS history_of_skin_disease,
    ''::text AS recently_given_birth,
    ''::text AS baby_is_symptomatic,
    ''::text AS case_is_breatfeeding,
    ''::text AS case_was_breastfeeding_three_weeks_prior_to_illness,
    ''::text AS title_clinical_care,
    ''::text AS type_of_care,
    ''::text AS case_is_isolated,
    ''::text AS health_facility_name,
    ''::text AS reason_for_hospitalization,
    ''::text AS reason_for_hospitalization_other,
    ''::text AS date_of_admission,
    ''::text AS inpatient_number,
    ''::text AS date_first_seen_at_facility,
    ''::text AS outpatient_number,
    ''::text AS treatment_given,
    ''::text AS treatment_given_other,
    ''::text AS outcome_of_patient,
    ''::text AS status_of_patient,
    ''::text AS date_of_discharge,
    ''::text AS date_of_death,
    ''::text AS place_of_death,
    ''::text AS food_name,
    ''::text AS food_source,
    ''::text AS food_consumption_date,
    ''::text AS food_participants_count,
    ''::text AS food_affected_participants_count,
    ''::text AS water_sources,
    ''::text AS water_sources_other,
    ''::text AS toilet_types,
    ''::text AS toilet_types_other,
    ''::text AS travel_origin_country,
    ''::text AS travel_origin_city,
    ''::text AS travel_departure_date,
    ''::text AS travel_destination_country,
    ''::text AS travel_destination_city,
    ''::text AS travel_arrival_date,
    ''::text AS animal_exposure_location,
    ''::text AS type_of_exposure,
    ''::text AS type_of_contact,
    ''::text AS contact_was_symptomatic,
    ''::text AS date_of_contact,
    ''::text AS frequency_of_contact,
    ''::text AS duration_of_contact,
    ''::text AS location_of_contact,
    ''::text AS location_of_contact_other,
    ''::text AS relationship_of_contact,
    ''::text AS relationship_of_contact_other,
    ''::text AS title_ppe,
    ''::text AS case_was_wearing_ppe,
    ''::text AS ppe_items_worn,
    ''::text AS ppe_breach,
    ''::text AS hand_hygiene_before_wearing_ppe,
    ''::text AS hand_hygiene_after_wearing_ppe,
    ''::text AS animal_species,
    ''::text AS animal_species_other,
    ''::text AS animal_exposure,
    ''::text AS animal_exposure_other,
    ''::text AS date_of_exposure,
    ''::text AS case_was_ill,
    ''::text AS description_of_event,
    ''::text AS location_of_event,
    ''::text AS date_of_event,
    ''::text AS description_of_illness,
    ''::text AS type_of_direct_contact,
    ''::text AS type_of_direct_contact_other,
    ''::text AS samples_were_collected,
    ''::text AS samples_collected,
    ''::text AS samples_collected_other,
    ''::text AS date_of_sample_collection,
    ''::text AS tests,
    ''::text AS test_other,
    ''::text AS result_of_laboratory_test,
    ''::text AS date_of_laboratory_testing_results,
    ''::text AS facility_name,
    ''::text AS facility_name_other,
    ''::text AS clade_of_mpox,
    ''::text AS day_of_follow_up,
    ''::text AS date_of_follow_up,
    ''::text AS contact_is_symptomatic,
    ''::text AS smallpox_mpox_vaccine_received,
    ''::text AS number_of_doses,
    ''::text AS date_of_first_dose,
    ''::text AS date_of_last_dose,
    ''::text AS brand_of_vaccine,
    ''::text AS brand_of_vaccine_other,
    ''::text AS route_of_admission,
    ''::text AS route_of_admission_other,
    ''::text AS reason_for_vaccination
UNION
SELECT
    id,
    CASE WHEN parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c051' ELSE parent_id END AS parent_id,
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
    'Monkey Pox'::text AS disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    location_of_investigation,
    location_of_investigation_other,
    type_of_case,
    family_name,
    given_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    date_of_birth,
    pregnant,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    type_of_residence,
    country,
    county,
    subcounty,
    ward,
    town_village_camp,
    landmark,
    marital_status,
    level_of_education,
    case_has_insurance_cover,
    title_parent_guradian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    symptoms,
    symptoms_other,
    fever_temperature,
    locations_of_lymphadenopathy,
    date_of_onset,
    start_location_of_rash,
    start_location_of_rash_other,
    spread_location_of_rash,
    spread_location_of_rash_other,
    number_of_lesions,
    development_of_rash,
    distribution_of_rash,
    history_of_immunosuppressive_conditions,
    hiv_positive,
    date_of_hiv_diagnosis,
    patient_on_art,
    history_of_skin_disease,
    recently_given_birth,
    baby_is_symptomatic,
    case_is_breatfeeding,
    case_was_breastfeeding_three_weeks_prior_to_illness,
    title_clinical_care,
    type_of_care,
    case_is_isolated,
    health_facility_name,
    reason_for_hospitalization,
    reason_for_hospitalization_other,
    date_of_admission,
    inpatient_number,
    date_first_seen_at_facility,
    outpatient_number,
    treatment_given,
    treatment_given_other,
    outcome_of_patient,
    status_of_patient,
    date_of_discharge,
    date_of_death,
    place_of_death,
    food_name,
    food_source,
    food_consumption_date,
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
    animal_exposure_location,
    type_of_exposure,
    type_of_contact,
    contact_was_symptomatic,
    date_of_contact,
    frequency_of_contact,
    duration_of_contact,
    location_of_contact,
    location_of_contact_other,
    relationship_of_contact,
    relationship_of_contact_other,
    title_ppe,
    case_was_wearing_ppe,
    ppe_items_worn,
    ppe_breach,
    hand_hygiene_before_wearing_ppe,
    hand_hygiene_after_wearing_ppe,
    animal_species,
    animal_species_other,
    animal_exposure,
    animal_exposure_other,
    date_of_exposure,
    case_was_ill,
    description_of_event,
    location_of_event,
    date_of_event,
    description_of_illness,
    type_of_direct_contact,
    type_of_direct_contact_other,
    samples_were_collected,
    samples_collected,
    samples_collected_other,
    date_of_sample_collection,
    tests,
    test_other,
    result_of_laboratory_test,
    date_of_laboratory_testing_results,
    facility_name,
    facility_name_other,
    clade_of_mpox,
    day_of_follow_up,
    date_of_follow_up,
    contact_is_symptomatic,
    smallpox_mpox_vaccine_received,
    number_of_doses,
    date_of_first_dose,
    date_of_last_dose,
    brand_of_vaccine,
    brand_of_vaccine_other,
    route_of_admission,
    route_of_admission_other,
    reason_for_vaccination
FROM {{ ref('int_mpox') }}