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
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'marital_status' ->> 'df_value')::text AS marital_status,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_is_pregnant' ->> 'df_value')::text AS case_is_pregnant,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_has_insurance_cover' ->> 'df_value')::text AS case_has_insurance_cover,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'level_of_education' ->> 'df_value')::text AS level_of_education,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'type_of_residence' ->> 'df_value')::text AS type_of_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
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
    (doc -> 'DForms' -> 'reporting_location' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS reporting_county,
    (doc -> 'DForms' -> 'reporting_location' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS reporting_subcounty,
    (doc -> 'DForms' -> 'reporting_location' -> 0 -> 'DFields' -> 'values' -> 'health_facility' ->> 'df_value')::text AS reporting_health_facility,
    (doc -> 'DForms' -> 'vaccination_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine' ->> 'df_value')::text AS vaccine,
    (doc -> 'DForms' -> 'vaccination_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccination_exists' ->> 'df_value')::text AS vaccination_exists,
    (doc -> 'DForms' -> 'vaccination_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_meningitis' ->> 'df_value')::text AS vaccine_meningitis,
    COALESCE((doc -> 'DForms' -> 'vaccination_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_doses' ->> 'df_value')::text, (doc -> 'DForms' -> 'vaccination_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_doses' ->> 'df_value')::text) AS vaccine_doses,
    COALESCE((doc -> 'DForms' -> 'vaccination_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_last_dose_date' ->> 'df_value')::text, (doc -> 'DForms' -> 'vaccination_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_last_dose_date' ->> 'df_value')::text) AS vaccine_last_dose_date,
    COALESCE((doc -> 'DForms' -> 'vaccination_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'source_of_vaccination_information' ->> 'df_value')::text, (doc -> 'DForms' -> 'vaccination_meningitis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_information_source' ->> 'df_value')::text) AS vaccine_information_source,
    (doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'lumbar_puncture_date' ->> 'df_value')::text AS lumbar_puncture_date,
    (doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'lumbar_puncture_time' ->> 'df_value')::text AS lumbar_puncture_time,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'celebrospinal_fluid_appearance' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'appearance_of_celebrospinal_fluid_sample' ->> 'df_value')::text) AS celebrospinal_fluid_appearance,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'inoculation_date' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'inoculation_date' ->> 'df_value')::text) AS inoculation_date,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'inoculation_time' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'inoculation_time' ->> 'df_value')::text) AS inoculation_time,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'sample_transport_medium' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'sample_transport_medium' ->> 'df_value')::text) AS sample_transport_medium,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'sample_transport_medium_others' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'sample_transport_medium_others' ->> 'df_value')::text) AS sample_transport_medium_others,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_test_requested' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_test_requested' ->> 'df_value')::text) AS laboratory_test_requested,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'reference_laboratory' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'reference_laboratory' ->> 'df_value')::text) AS reference_laboratory,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_information_exists' ->> 'df_value')::text AS laboratory_information_exists,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text AS laboratory_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'appearance_of_sttol_sample' ->> 'df_value')::text AS appearance_of_sttol_sample,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_tests' ->> 'df_value')::text AS laboratory_tests,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_tests_other' ->> 'df_value')::text AS laboratory_tests_other,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'title_results' ->> 'df_value')::text AS title_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'leucocytes_results' ->> 'df_value')::text AS leucocytes_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'pn_results' ->> 'df_value')::text AS pn_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'lymp_results' ->> 'df_value')::text AS lymp_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'gram_results' ->> 'df_value')::text AS gram_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'rdt_results' ->> 'df_value')::text AS rdt_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'serology_results' ->> 'df_value')::text AS serology_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'laboratory_results_other' ->> 'df_value')::text AS laboratory_results_other,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_of_laboratory_results' ->> 'df_value')::text AS date_of_laboratory_results,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'title_laboratory_facility' ->> 'df_value')::text AS title_laboratory_facility,
    (doc -> 'DForms' -> 'laboratory_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory' ->> 'df_value')::text AS name_of_laboratory,
    COALESCE((doc -> 'DForms' -> 'laboratory_sample_meningitis' -> 0 -> 'DFields' -> 'values' -> 'patient_on_treatment' ->> 'df_value')::text, (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'patient_on_treatment' ->> 'df_value')::text) AS patient_on_treatment,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'patient_on_treatment_specify' ->> 'df_value')::text AS patient_on_treatment_specify,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'name_of_health_facility' ->> 'df_value')::text AS name_of_health_facility,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_first_seen_at_facility' ->> 'df_value')::text AS date_first_seen_at_facility,
    COALESCE((doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'admission_date' ->> 'df_value')::text, (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text) AS admission_date,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'health_facility' ->> 'df_value')::text AS clinical_health_facility,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'patient_status' ->> 'df_value')::text AS patient_status,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'status_of_case' ->> 'df_value')::text AS status_of_case,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_case' ->> 'df_value')::text AS outcome_of_case,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'place_of_death' ->> 'df_value')::text AS place_of_death,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_meningitis' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care
FROM {{ source('couchdb', 'couchdb') }}
WHERE 
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = '4e3defa0-039f-11ef-a3ca-7532751d65a4'
