{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
    ]
)}}

SELECT
    mform_id,
    form_id,
    case_unique_id,
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
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_group,
    date_of_birth,
    phone_number,
    occupation,
    occupation_other,
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
    laboratory_facility_name_other
FROM {{ ref('int_respiratory_syndrome') }}