SELECT
    (doc ->> '_id')::text AS doc_id,
    (doc ->> '_rev')::text AS doc_rev,
    (doc ->> 'type')::text AS doc_type,
    COALESCE((doc -> 'DFields' -> 'values' -> 'unique_id_of_case' ->> 'df_value')::text, (doc ->> 'ident')::text) AS case_unique_id,
    (doc ->> 'mform_id')::text AS mform_id,
    (doc ->> 'fform_id')::text AS form_id,
    (doc ->> 'df_complete')::text AS df_complete,
    (doc ->> 'df_submitted')::text AS df_submitted,
    (doc ->> 'df_parent_complete')::text AS df_parent_complete,
    (doc ->> 'event_ident')::text AS event_ident,
    (doc ->> 'mform_event')::text AS mform_event,
    (doc ->> 'parent_ident')::text AS parent_ident,
    (doc -> 'created_scope' ->> 'user_role')::text AS created_role,
    (doc -> 'modified_scope' ->> 'user_role')::text AS modified_role,
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
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'marital_status' ->> 'df_value')::text AS marital_status,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'case_has_insurance_cover' ->> 'df_value')::text AS case_has_insurance_cover,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'ward' ->> 'df_value')::text AS ward,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'type_of_residence' ->> 'df_value')::text AS type_of_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'level_of_education' ->> 'df_value')::text AS level_of_education,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    COALESCE((doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guradian' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guardian' ->> 'df_value')::text) AS title_parent_guradian,
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
    (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccinated' ->> 'df_value')::text AS vaccinated,
    COALESCE((doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'number_of_doses' ->> 'df_value')::text, (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccine_doses' ->> 'df_value')::text) AS vaccine_doses,
    COALESCE((doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_last_vaccination' ->> 'df_value')::text, (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccination_date' ->> 'df_value')::text) AS vaccination_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'record_exposure' ->> 'df_value')::text AS record_exposure,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'type_of_exposure' ->> 'df_value')::text AS exposure_type,
    COALESCE((doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'water_sources' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'water_source' ->> 'df_value')::text) AS water_sources,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'water_sources_other' ->> 'df_value')::text AS water_sources_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'other_water_source' ->> 'df_value')::text AS other_water_source,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'other_water_source_treated' ->> 'df_value')::text AS other_water_source_treated,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'other_water_source_treatment_method' ->> 'df_value')::text AS other_water_source_treatment_method,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'other_water_source_treatment_method_other' ->> 'df_value')::text AS other_water_source_treatment_method_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_name' ->> 'df_value')::text AS food_name,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_consumption_date' ->> 'df_value')::text AS food_consumption_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_source' ->> 'df_value')::text AS food_source,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_participants_count' ->> 'df_value')::text AS food_participants_count,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_affected_participants_count' ->> 'df_value')::text AS food_affected_participants_count,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'toilet_types' ->> 'df_value')::text AS toilet_types,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'toilet_types_other' ->> 'df_value')::text AS toilet_types_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_consumed' ->> 'df_value')::text AS food_consumed,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'external_food_consumed' ->> 'df_value')::text AS external_food_consumed,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'name_of_vendor_or_place' ->> 'df_value')::text AS name_of_vendor_or_place,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'method_of_waste_disporsal' ->> 'df_value')::text AS method_of_waste_disporsal,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'method_of_waste_disporsal_other' ->> 'df_value')::text AS method_of_waste_disporsal_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'waste_disporsal_shared' ->> 'df_value')::text AS waste_disporsal_shared,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'handwashing_moments' ->> 'df_value')::text AS handwashing_moments,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'handwashing_moments_other' ->> 'df_value')::text AS handwashing_moments_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'handwashing_with_soap' ->> 'df_value')::text AS handwashing_with_soap,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'contact_with_case' ->> 'df_value')::text AS contact_with_case,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'type_of_contact' ->> 'df_value')::text AS type_of_contact,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'place_of_interaction' ->> 'df_value')::text AS place_of_interaction,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'attended_social_gathering' ->> 'df_value')::text AS attended_social_gathering,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'location_of_social_gathering' ->> 'df_value')::text AS location_of_social_gathering,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_social_gathering' ->> 'df_value')::text AS date_of_social_gathering,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'type_of_social_event' ->> 'df_value')::text AS type_of_social_event,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'type_of_social_event_other' ->> 'df_value')::text AS type_of_social_event_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'description_of_social_event' ->> 'df_value')::text AS description_of_social_event,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'case_travelled' ->> 'df_value')::text AS case_travelled,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_departure_date' ->> 'df_value')::text AS travel_departure_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_arrival_date' ->> 'df_value')::text AS travel_arrival_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_country' ->> 'df_value')::text AS travel_origin_country,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_city' ->> 'df_value')::text AS travel_origin_city,
    COALESCE((doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_country' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'destination_country' ->> 'df_value')::text) AS travel_destination_country,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'destination_county' ->> 'df_value')::text AS destination_county,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'destination_subcounty' ->> 'df_value')::text AS destination_subcounty,
    COALESCE((doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'destination_city_town_village_camp' ->> 'df_value')::text, (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_city' ->> 'df_value')::text) AS travel_destination_city,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'symptoms_other' ->> 'df_value')::text AS symptoms_other,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'level_of_hydration' ->> 'df_value')::text AS level_of_hydration,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'antibiotics_taken' ->> 'df_value')::text AS antibiotics_taken,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'antibiotic_name' ->> 'df_value')::text AS antibiotic_name,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'health_facility_name' ->> 'df_value')::text AS health_facility_name,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text AS date_of_admission,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_patient' ->> 'df_value')::text AS outcome_of_patient,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'place_of_death' ->> 'df_value')::text AS place_of_death,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'status_of_patient' ->> 'df_value')::text AS status_of_patient,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'were_samples_collected' ->> 'df_value')::text AS were_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text AS laboratory_samples_collected,
    COALESCE((doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_other' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_others' ->> 'df_value')::text) AS laboratory_samples_collected_others,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'rdt_done' ->> 'df_value')::text AS rdt_done,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'rdt_results' ->> 'df_value')::text AS rdt_results,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'culture_done' ->> 'df_value')::text AS culture_done,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_culture_test' ->> 'df_value')::text AS date_of_culture_test,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'title_laboratory_facility' ->> 'df_value')::text AS title_laboratory_facility,
    COALESCE((doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory_facility' ->> 'df_value')::text, (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory' ->> 'df_value')::text) AS name_of_laboratory,
    COALESCE((doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_facility_mfl_number' ->> 'df_value')::text, (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'health_facility_mfl_number' ->> 'df_value')::text) AS health_facility_mfl_number,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'title_results' ->> 'df_value')::text AS title_results,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_culture_result' ->> 'df_value')::text AS date_of_culture_result,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'culture_result_diarrhoeal' ->> 'df_value')::text AS culture_result_diarrhoeal
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = 'a6ab4090-e084-11ee-9e98-6dbc0bbe50a8'
