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
    'Respiratory Syndrome'::text AS syndrome,
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
    case_is_pregnant,
    phone_number,
    marital_status,
    occupation,
    occupation_other,
    level_of_education,
    case_has_insurance_cover,
    country,
    county,
    subcounty,
    ward,
    town_village_camp,
    landmark,
    title_residence,
    type_of_residence,
    title_parent_guardian,
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
    vaccination_received,
    vaccination_received_other,
    name_of_vaccine,
    date_of_vaccination,
    number_of_doses,
    case_travelled,
    case_risk_behaviours,
    case_chronic_disease_other,
    case_risk_behaviours_other,
    case_visited_health_facility,
    case_underlying_medical_conditions,
    case_has_underlying_medical_condition,
    case_contact_with_suspected_or_confirmed_case,
    date_of_first_contact_with_healthcare_system,
    symptoms,
    symptoms_other,
    date_of_onset,
    title_clinical_care,
    type_of_care,
    health_facility_name,
    health_facility_mfl_number,
    date_of_admission,
    inpatient_number,
    outpatient_number,
    outcome_of_patient,
    date_of_death,
    status_of_patient,
    date_of_discharge,
    were_laboratory_samples_collected,
    laboratory_samples_collected,
    date_of_sample_collection,
    laboratory_test_conducted,
    laboratory_test_conducted_other,
    date_of_laboratory_testing,
    date_of_laboratory_testing_results,
    result_of_laboratory_test,
    laboratory_facility_name,
    laboratory_facility_name_other,
    (1)::integer AS suspected,
    (CASE WHEN were_laboratory_samples_collected = 'Yes' THEN 1 WHEN laboratory_test_conducted IS NOT NULL THEN 1 WHEN laboratory_test_conducted_other IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN result_of_laboratory_test = 'Positive' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN date_of_admission IS NOT NULL AND date_of_admission != '' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN date_of_discharge IS NOT NULL AND date_of_discharge != '' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN date_of_death IS NOT NULL AND date_of_death != '' THEN 1 ELSE 0 END)::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_respiratory_syndrome') }}
