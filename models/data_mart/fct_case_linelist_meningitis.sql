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
    phone_number,
    occupation,
    occupation_other,
    marital_status,
    case_is_pregnant,
    case_has_insurance_cover,
    level_of_education,
    type_of_residence,
    ward,
    country,
    county,
    subcounty,
    town_village_camp,
    landmark,
    title_residence,
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
    reporting_county,
    reporting_subcounty,
    reporting_health_facility,
    vaccine,
    vaccination_exists,
    vaccine_meningitis,
    vaccine_doses,
    vaccine_last_dose_date,
    vaccine_information_source,
    lumbar_puncture_date,
    lumbar_puncture_time,
    celebrospinal_fluid_appearance,
    inoculation_date,
    inoculation_time,
    sample_transport_medium,
    sample_transport_medium_others,
    laboratory_test_requested,
    reference_laboratory,
    laboratory_information_exists,
    laboratory_samples_collected,
    appearance_of_sttol_sample,
    laboratory_tests,
    laboratory_tests_other,
    title_results,
    leucocytes_results,
    pn_results,
    lymp_results,
    gram_results,
    rdt_results,
    serology_results,
    laboratory_results_other,
    date_of_laboratory_results,
    title_laboratory_facility,
    name_of_laboratory,
    patient_on_treatment,
    patient_on_treatment_specify,
    symptoms,
    date_of_onset,
    clinical_health_facility,
    name_of_health_facility,
    date_first_seen_at_facility,
    admission_date,
    patient_status,
    status_of_case,
    outcome_of_case,
    date_of_discharge,
    date_of_death,
    place_of_death,
    inpatient_number,
    outpatient_number,
    title_clinical_care,
    type_of_care,
    (1)::integer AS suspected,
    (CASE WHEN laboratory_test_requested IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN patient_status IN ('Admitted', 'Discharged', 'Dead') THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN patient_status = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_meningitis') }}
