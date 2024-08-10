SELECT
    (doc ->> 'mform_id')::text AS mform_id,
    (doc ->> 'fform_id')::text AS form_id,
    (doc ->> 'ident')::text AS case_unique_id,
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
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'family' ->> 'df_value')::text AS family,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'given' ->> 'df_value')::text AS given,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'sex' ->> 'df_value')::text AS sex,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'date_of_birth' ->> 'df_value')::text AS date_of_birth,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_months' ->> 'df_value')::text AS age_months,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_days' ->> 'df_value')::text AS age_days,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_is_pregnant' ->> 'df_value')::text AS case_is_pregnant,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guradian' ->> 'df_value')::text AS title_parent_guradian,
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
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'symptoms_other' ->> 'df_value')::text AS symptoms_other,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'fever_temperature' ->> 'df_value')::text AS fever_temperature,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'locations_of_lymphadenopathy' ->> 'df_value')::text AS locations_of_lymphadenopathy,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'case_is_isolated' ->> 'df_value')::text AS case_is_isolated,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'health_facility_name' ->> 'df_value')::text AS health_facility_name,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text AS date_of_admission,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_first_seen_at_facility' ->> 'df_value')::text AS date_first_seen_at_facility,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_patient' ->> 'df_value')::text AS outcome_of_patient,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'status_of_patient' ->> 'df_value')::text AS status_of_patient,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'place_of_death' ->> 'df_value')::text AS place_of_death,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'type_of_exposure' ->> 'df_value')::text AS type_of_exposure,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'type_of_contact' ->> 'df_value')::text AS type_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'contact_was_symptomatic' ->> 'df_value')::text AS contact_was_symptomatic,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_contact' ->> 'df_value')::text AS date_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'frequency_of_contact' ->> 'df_value')::text AS frequency_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'duration_of_contact' ->> 'df_value')::text AS duration_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'location_of_contact' ->> 'df_value')::text AS location_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'location_of_contact_other' ->> 'df_value')::text AS location_of_contact_other,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'relationship_of_contact' ->> 'df_value')::text AS relationship_of_contact,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'relationship_of_contact_other' ->> 'df_value')::text AS relationship_of_contact_other,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'title_ppe' ->> 'df_value')::text AS title_ppe,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'case_was_wearing_ppe' ->> 'df_value')::text AS case_was_wearing_ppe,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'ppe_items_worn' ->> 'df_value')::text AS ppe_items_worn,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'ppe_breach' ->> 'df_value')::text AS ppe_breach,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'hand_hygiene_before_wearing_ppe' ->> 'df_value')::text AS hand_hygiene_before_wearing_ppe,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'hand_hygiene_after_wearing_ppe' ->> 'df_value')::text AS hand_hygiene_after_wearing_ppe,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'animal_species' ->> 'df_value')::text AS animal_species,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'animal_species_other' ->> 'df_value')::text AS animal_species_other,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure' ->> 'df_value')::text AS animal_exposure,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_other' ->> 'df_value')::text AS animal_exposure_other,
    (doc -> 'DForms' -> 'exposure_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_exposure' ->> 'df_value')::text AS date_of_exposure,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_were_collected' ->> 'df_value')::text AS samples_were_collected,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text AS samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_other' ->> 'df_value')::text AS samples_collected_other,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_tests' ->> 'df_value')::text AS tests,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_test_other' ->> 'df_value')::text AS test_other,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'result_of_laboratory_test' ->> 'df_value')::text AS result_of_laboratory_test,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_laboratory_testing_results' ->> 'df_value')::text AS date_of_laboratory_testing_results,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_facility_name' ->> 'df_value')::text AS facility_name,
    (doc -> 'DForms' -> 'laboratory_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'laboratory_facility_name_other' ->> 'df_value')::text AS facility_name_other,
    (doc -> 'DForms' -> 'vaccination_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'smallpox_mpox_vaccine_received' ->> 'df_value')::text AS smallpox_mpox_vaccine_received,
    (doc -> 'DForms' -> 'vaccination_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'number_of_doses' ->> 'df_value')::text AS number_of_doses,
    (doc -> 'DForms' -> 'vaccination_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'date_of_first_dose' ->> 'df_value')::text AS date_of_first_dose,
    (doc -> 'DForms' -> 'vaccination_information_monkey_pox' -> 0 -> 'DFields' -> 'values' -> 'brand_of_vaccine' ->> 'df_value')::text AS brand_of_vaccine
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc -> 'DFields' -> 'values' -> 'disease' ->> 'df_value')::text = 'Monkey Pox'
AND
    (doc -> 'ident') IS NOT NULL