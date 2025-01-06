SELECT
    (doc ->> '_id')::text AS doc_id,
    (doc ->> '_rev')::text AS doc_rev,
    (doc ->> 'type')::text AS doc_type,
    (doc ->> 'df_complete')::text AS df_complete,
    (doc ->> 'df_submitted')::text AS df_submitted,
    (doc ->> 'df_parent_complete')::text AS df_parent_complete,
    (doc ->> 'event_ident')::text AS event_ident,
    (doc ->> 'mform_event')::text AS mform_event,
    (doc ->> 'parent_ident')::text AS parent_ident,
    (doc -> 'created_scope' ->> 'user_role')::text AS created_role,
    (doc -> 'modified_scope' ->> 'user_role')::text AS modified_role,
    (doc ->> 'mform_id')::text AS mform_id,
    (doc ->> 'fform_id')::text AS form_id,
    COALESCE((doc -> 'DFields' -> 'values' -> 'unique_id_of_case' ->> 'df_value')::text, (doc ->> 'ident')::text) AS case_unique_id,
    (doc ->> 'created_username')::text AS created_username,
    (doc ->> 'created_timestamp')::text AS created_timestamp,
    (doc ->> 'modified_username')::text AS modified_username,
    (doc ->> 'modified_timestamp')::text AS modified_timestamp,
    (doc -> 'location' ->> 'accuracy')::text AS location_accuracy,
    (doc -> 'location' ->> 'latitude')::text AS location_latitude,
    (doc -> 'location' ->> 'longitude')::text AS location_longitude,
    (doc -> 'DFields' -> 'values' -> 'syndrome' ->> 'df_value')::text AS syndrome,
    (doc -> 'DFields' -> 'values' -> 'disease' ->> 'df_value')::text AS disease,
    (doc -> 'DFields' -> 'values' -> 'EPID' ->> 'df_value')::text AS epid,
    (doc -> 'DFields' -> 'values' -> 'date_of_investigation' ->> 'df_value')::text AS date_of_investigation,
    (doc -> 'DFields' -> 'values' -> 'location_of_investigation' ->> 'df_value')::text AS location_of_investigation,
    (doc -> 'DFields' -> 'values' -> 'location_of_investigation_other' ->> 'df_value')::text AS location_of_investigation_other,
    (doc -> 'DFields' -> 'values' -> 'type_of_case' ->> 'df_value')::text AS type_of_case,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'given' ->> 'df_value')::text AS given,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'family' ->> 'df_value')::text AS family,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'sex' ->> 'df_value')::text AS sex,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'date_of_birth' ->> 'df_value')::text AS date_of_birth,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_months' ->> 'df_value')::text AS age_months,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_days' ->> 'df_value')::text AS age_days,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_is_pregnant' ->> 'df_value')::text AS case_is_pregnant,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'marital_status' ->> 'df_value')::text AS marital_status,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'level_of_education' ->> 'df_value')::text AS level_of_education,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_has_insurance_cover' ->> 'df_value')::text AS case_has_insurance_cover,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'type_of_residence' ->> 'df_value')::text AS type_of_residence,
    COALESCE((doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guradian' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guardian' ->> 'df_value')::text) AS title_parent_guardian,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_family_name' ->> 'df_value')::text AS guardian_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_given_name' ->> 'df_value')::text AS guardian_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_phone_number' ->> 'df_value')::text AS guardian_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_respondent' ->> 'df_value')::text AS title_respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent' ->> 'df_value')::text AS respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_family_name' ->> 'df_value')::text AS respondent_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_given_name' ->> 'df_value')::text AS respondent_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_address' ->> 'df_value')::text AS respondent_address,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_phone_number' ->> 'df_value')::text AS respondent_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_relationship' ->> 'df_value')::text AS respondent_relationship,
    (doc -> 'DForms' -> 'vaccination_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'vaccination_received' ->> 'df_value')::text AS vaccination_received,
    (doc -> 'DForms' -> 'vaccination_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'vaccination_received_other' ->> 'df_value')::text AS vaccination_received_other,
    (doc -> 'DForms' -> 'vaccination_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'name_of_vaccine' ->> 'df_value')::text AS name_of_vaccine,
    (doc -> 'DForms' -> 'vaccination_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_vaccination' ->> 'df_value')::text AS date_of_vaccination,
    (doc -> 'DForms' -> 'vaccination_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'number_of_doses' ->> 'df_value')::text AS number_of_doses,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_travelled' ->> 'df_value')::text AS case_travelled,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_risk_behaviours' ->> 'df_value')::text AS case_risk_behaviours,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_chronic_disease_other' ->> 'df_value')::text AS case_chronic_disease_other,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_risk_behaviours_other' ->> 'df_value')::text AS case_risk_behaviours_other,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_visited_health_facility' ->> 'df_value')::text AS case_visited_health_facility,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_underlying_medical_conditions' ->> 'df_value')::text AS case_underlying_medical_conditions,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_has_underlying_medical_condition' ->> 'df_value')::text AS case_has_underlying_medical_condition,
    (doc -> 'DForms' -> 'risk_factors_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'case_contact_with_suspected_or_confirmed_case' ->> 'df_value')::text AS case_contact_with_suspected_or_confirmed_case,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_first_contact_with_healthcare_system' ->> 'df_value')::text AS date_of_first_contact_with_healthcare_system,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'symptoms_other' ->> 'df_value')::text AS symptoms_other,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care,
    COALESCE((doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'health_facility_name' ->> 'df_value')::text, (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'name_of_health_facility' ->> 'df_value')::text) AS health_facility_name,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'health_facility_mfl_number' ->> 'df_value')::text AS health_facility_mfl_number,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text AS date_of_admission,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_patient' ->> 'df_value')::text AS outcome_of_patient,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'status_of_patient' ->> 'df_value')::text AS status_of_patient,
    (doc -> 'DForms' -> 'clinical_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'were_laboratory_samples_collected' ->> 'df_value')::text AS were_laboratory_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' -> 'df_value')::text AS laboratory_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'laboratory_test_conducted' ->> 'df_value')::text AS laboratory_test_conducted,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'laboratory_test_conducted_other' ->> 'df_value')::text AS laboratory_test_conducted_other,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_laboratory_testing' ->> 'df_value')::text AS date_of_laboratory_testing,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'date_of_laboratory_testing_results' ->> 'df_value')::text AS date_of_laboratory_testing_results,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'result_of_laboratory_test' ->> 'df_value')::text AS result_of_laboratory_test,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'laboratory_facility_name' ->> 'df_value')::text AS laboratory_facility_name,
    (doc -> 'DForms' -> 'laboratory_information_respiratory_syndrome' -> 0 -> 'DFields' -> 'values' -> 'laboratory_facility_name_other' ->> 'df_value')::text AS laboratory_facility_name_other
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = 'adbf8170-effc-11ee-9657-d59df5fc939e'
