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
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'marital_status' ->> 'df_value')::text AS marital_status,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_is_pregnant' ->> 'df_value')::text AS case_is_pregnant,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_has_insurance_cover' ->> 'df_value')::text AS case_has_insurance_cover,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'level_of_education' ->> 'df_value')::text AS level_of_education,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'type_of_residence' ->> 'df_value')::text AS type_of_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    COALESCE((doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guradian' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guardian' ->> 'df_value')::text) AS title_parent_guardian,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_family_name' ->> 'df_value')::text AS guardian_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_given_name' ->> 'df_value')::text AS guardian_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_phone_number' ->> 'df_value')::text AS guardian_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_respondent' ->> 'df_value')::text AS title_respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent' ->> 'df_value')::text AS respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_family_name' ->> 'df_value')::text AS respondent_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_given_name' ->> 'df_value')::text AS respondent_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_phone_number' ->> 'df_value')::text AS respondent_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_relationship' ->> 'df_value')::text AS respondent_relationship,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'relationship_other' ->> 'df_value')::text AS respondent_relationship_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_address' ->> 'df_value')::text AS respondent_address,
    (doc -> 'DForms' -> 'case_contact' -> 0 -> 'DFields' -> 'values' -> 'surname' ->> 'df_value')::text AS contact_surname,
    (doc -> 'DForms' -> 'case_contact' -> 0 -> 'DFields' -> 'values' -> 'given_name' ->> 'df_value')::text AS contact_given_name,
    (doc -> 'DForms' -> 'case_contact' -> 0 -> 'DFields' -> 'values' -> 'gender' ->> 'df_value')::text AS contact_gender,
    (doc -> 'DForms' -> 'case_contact' -> 0 -> 'DFields' -> 'values' -> 'age' ->> 'df_value')::text AS contact_age,
    (doc -> 'DForms' -> 'case_outcome' -> 0 -> 'DFields' -> 'values' -> 'final_case_classification' ->> 'df_value')::text AS outcome_final_case_classification,
    (doc -> 'DForms' -> 'case_outcome' -> 0 -> 'DFields' -> 'values' -> 'final_patient_status' ->> 'df_value')::text AS outcome_final_patient_status,
    (doc -> 'DForms' -> 'case_outcome' -> 0 -> 'DFields' -> 'values' -> 'final_patient_status_other' ->> 'df_value')::text AS outcome_final_patient_status_other,
    (doc -> 'DForms' -> 'case_outcome' -> 0 -> 'DFields' -> 'values' -> 'reason_for_referral' ->> 'df_value')::text AS outcome_reason_for_referral,
    (doc -> 'DForms' -> 'case_outcome' -> 0 -> 'DFields' -> 'values' -> 'date_of_outcome' ->> 'df_value')::text AS outcome_date_of_outcome,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'record_exposure' ->> 'df_value')::text AS record_exposure,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'exposure_type' ->> 'df_value')::text AS exposure_type,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'type_of_exposure' ->> 'df_value')::text AS type_of_exposure,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_name' ->> 'df_value')::text AS food_name,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_consumption_date' ->> 'df_value')::text AS food_consumption_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_source' ->> 'df_value')::text AS food_source,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_participants_count' ->> 'df_value')::text AS food_participants_count,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_affected_participants_count' ->> 'df_value')::text AS food_affected_participants_count,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'water_sources' ->> 'df_value')::text AS water_sources,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'water_sources_other' ->> 'df_value')::text AS water_sources_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'toilet_types' ->> 'df_value')::text AS toilet_types,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'toilet_types_other' ->> 'df_value')::text AS toilet_types_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_country' ->> 'df_value')::text AS travel_origin_country,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_city' ->> 'df_value')::text AS travel_origin_city,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_departure_date' ->> 'df_value')::text AS travel_departure_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_country' ->> 'df_value')::text AS travel_destination_country,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_city' ->> 'df_value')::text AS travel_destination_city,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_arrival_date' ->> 'df_value')::text AS travel_arrival_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure' ->> 'df_value')::text AS animal_exposure,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_other' ->> 'df_value')::text AS animal_exposure_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_species' ->> 'df_value')::text AS animal_species,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_species_other' ->> 'df_value')::text AS animal_species_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_location' ->> 'df_value')::text AS animal_exposure_location,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_date' ->> 'df_value')::text AS animal_exposure_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'type_of_case_contact' ->> 'df_value')::text AS type_of_case_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'type_of_direct_contact' ->> 'df_value')::text AS type_of_direct_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'type_of_direct_contact_other' ->> 'df_value')::text AS type_of_direct_contact_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'case_was_symptomatic_during_contact' ->> 'df_value')::text AS case_was_symptomatic_during_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'date_of_contact' ->> 'df_value')::text AS date_of_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'frequency_of_contact' ->> 'df_value')::text AS frequency_of_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'duration_of_contact' ->> 'df_value')::text AS duration_of_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'location_of_contact' ->> 'df_value')::text AS location_of_contact,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'relationship' ->> 'df_value')::text AS relationship,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'relationship_other' ->> 'df_value')::text AS relationship_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'title_ppe' ->> 'df_value')::text AS title_ppe,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'case_was_wearing_ppe' ->> 'df_value')::text AS case_was_wearing_ppe,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'ppe_items_worn' ->> 'df_value')::text AS ppe_items_worn,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'ppe_breach' ->> 'df_value')::text AS ppe_breach,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'hand_hygiene_before_wearing_ppe' ->> 'df_value')::text AS hand_hygiene_before_wearing_ppe,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'hand_hygiene_after_wearing_ppe' ->> 'df_value')::text AS hand_hygiene_after_wearing_ppe,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'description_of_event' ->> 'df_value')::text AS description_of_event,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'location_of_event' ->> 'df_value')::text AS location_of_event,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'date_of_event' ->> 'df_value')::text AS date_of_event,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'case_was_ill' ->> 'df_value')::text AS case_was_ill,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'description_of_illness' ->> 'df_value')::text AS description_of_illness,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'case_seen_at_facility' ->> 'df_value')::text AS case_seen_at_facility,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'date_first_seen_at_facility' ->> 'df_value')::text AS date_first_seen_at_facility,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'admitted' ->> 'df_value')::text AS admitted,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'health_facility_name' ->> 'df_value')::text AS health_facility_name,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'admission_date' ->> 'df_value')::text AS admission_date,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'discharge_date' ->> 'df_value')::text AS discharge_date,
    (doc -> 'DForms' -> 'case_hospitalization' -> 0 -> 'DFields' -> 'values' -> 'patient_status' ->> 'df_value')::text AS patient_status,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text AS laboratory_samples_collected,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_others' ->> 'df_value')::text AS laboratory_samples_collected_others,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'sample_date' ->> 'df_value')::text AS sample_date,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'rdt_done' ->> 'df_value')::text AS rdt_done,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'samples_sent_to_laboratory' ->> 'df_value')::text AS samples_sent_to_laboratory,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'laboratory_name' ->> 'df_value')::text AS laboratory_name,
    (doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'sample_sent_to_lab_date' ->> 'df_value')::text AS sample_sent_to_lab_date,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms_other' ->> 'df_value')::text AS symptoms_other,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms_unexplained_bleeding' ->> 'df_value')::text AS symptoms_unexplained_bleeding,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms_start_date' ->> 'df_value')::text AS symptoms_start_date,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'title_symptoms_start_location' ->> 'df_value')::text AS title_symptoms_start_location,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms_start_county' ->> 'df_value')::text AS symptoms_start_county,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'symptoms_start_subcounty' ->> 'df_value')::text AS symptoms_start_subcounty,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'City / Town / Village / Camp' ->> 'df_value')::text AS symptoms_start_city_town_village_camp,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'outcome' ->> 'df_value')::text AS outcome,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'name_of_health_facility' ->> 'df_value')::text AS name_of_health_facility,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text AS date_of_admission,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'ward_of_admission' ->> 'df_value')::text AS ward_of_admission,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_case' ->> 'df_value')::text AS outcome_of_case,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'status_of_patient' ->> 'df_value')::text AS status_of_patient,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'place_of_death' ->> 'df_value')::text AS place_of_death,
    (doc -> 'DForms' -> 'clinical_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'place_of_burial' ->> 'df_value')::text AS place_of_burial,
    COALESCE((doc -> 'DForms' -> 'case_lab_information' -> 0 -> 'DFields' -> 'values' -> 'laboratory_sample_collected' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text) AS laboratory_sample_collected,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_other' ->> 'df_value')::text AS laboratory_samples_collected_other,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'laboratory_tests' ->> 'df_value')::text AS laboratory_tests,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'rdt_results' ->> 'df_value')::text AS rdt_results,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'title_laboratory_facility' ->> 'df_value')::text AS title_laboratory_facility,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'type_of_laboratory_facility' ->> 'df_value')::text AS type_of_laboratory_facility,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory_facility' ->> 'df_value')::text AS name_of_laboratory_facility,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory_facility_other' ->> 'df_value')::text AS name_of_laboratory_facility_other,
    (doc -> 'DForms' -> 'laboratory_information_vhf' -> 0 -> 'DFields' -> 'values' -> 'date_of_laboratory_testing_results' ->> 'df_value')::text AS date_of_laboratory_testing_results,
    (doc -> 'DForms' -> 'vaccination_ebola' -> 0 -> 'DFields' -> 'values' -> 'ebola_vaccine_received' ->> 'df_value')::text AS vaccination_ebola_vaccine_received,
    (doc -> 'DForms' -> 'vaccination_ebola' -> 0 -> 'DFields' -> 'values' -> 'name_of_vaccine' ->> 'df_value')::text AS vaccination_ebola_name_of_vaccine,
    (doc -> 'DForms' -> 'vaccination_ebola' -> 0 -> 'DFields' -> 'values' -> 'number_of_doses' ->> 'df_value')::text AS vaccination_ebola_number_of_doses,
    (doc -> 'DForms' -> 'vaccination_ebola' -> 0 -> 'DFields' -> 'values' -> 'date_of_vaccination' ->> 'df_value')::text AS vaccination_ebola_date_of_vaccination,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'day_of_follow_up' ->> 'df_value')::text AS contact_follow_up_day_of_follow_up,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'date_of_follow_up' ->> 'df_value')::text AS contact_follow_up_date_of_follow_up,
    (doc -> 'DForms' -> 'contact_follow_up' -> 0 -> 'DFields' -> 'values' -> 'contact_is_symptomatic' ->> 'df_value')::text AS contact_follow_up_contact_is_symptomatic
FROM {{ source('couchdb', 'couchdb') }}
WHERE 
    (doc ->> 'type') = 'dform'
AND
    (doc ->> 'mform_id')::text = 'a78b43f0-e4f0-11ee-a969-7765f1f98ba9'
