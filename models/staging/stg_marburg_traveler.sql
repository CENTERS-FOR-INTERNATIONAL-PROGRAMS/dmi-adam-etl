SELECT
    (doc ->> '_id')::text AS doc_id,
    (doc ->> '_rev')::text AS doc_rev,
    (doc ->> 'type')::text AS doc_type,
    (doc ->> 'df_complete')::text AS doc_df_complete,
    (doc ->> 'df_submitted')::text AS doc_df_submitted,
    (doc ->> 'df_parent_complete')::text AS doc_df_parent_complete,
    (doc ->> 'event_ident')::text AS doc_event_ident,
    (doc ->> 'mform_event')::text AS doc_mform_event,
    (doc ->> 'parent_ident')::text AS doc_parent_ident,
    (doc -> 'created_scope' ->> 'user_role')::text AS created_scope_user_role,
    (doc -> 'modified_scope' ->> 'user_role')::text AS modified_scope_user_role,
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
    (doc -> 'DFields' -> 'values' -> 'name_of_traveller' ->> 'df_value')::text AS name_of_traveller,
    (doc -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DFields' -> 'values' -> 'sex_of_traveller' ->> 'df_value')::text AS sex_of_traveller,
    (doc -> 'DFields' -> 'values' -> 'nationality_of_traveller' ->> 'df_value')::text AS nationality_of_traveller,
    (doc -> 'DFields' -> 'values' -> 'id_number' ->> 'df_value')::text AS id_number,
    (doc -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DFields' -> 'values' -> 'email_address' ->> 'df_value')::text AS email_address,
    (doc -> 'DForms' -> 'traveller_marburg_official' -> 0 -> 'DFields' -> 'values' -> 'temperature_of_traveller' ->> 'df_value')::text AS temperature_of_traveller,
    (doc -> 'DForms' -> 'traveller_marburg_official' -> 0 -> 'DFields' -> 'values' -> 'classification_of_traveller' ->> 'df_value')::text AS classification_of_traveller,
    (doc -> 'DForms' -> 'traveller_marburg_official' -> 0 -> 'DFields' -> 'values' -> 'traveller_action' ->> 'df_value')::text AS traveller_action,
    (doc -> 'DForms' -> 'traveller_marburg_official' -> 0 -> 'DFields' -> 'values' -> 'traveller_action_other' ->> 'df_value')::text AS traveller_action_other,
    (doc -> 'DForms' -> 'traveller_marburg_official' -> 0 -> 'DFields' -> 'values' -> 'traveller_date_of_checking' ->> 'df_value')::text AS traveller_date_of_checking,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'three_weeks_exposure_title' ->> 'df_value')::text AS three_weeks_exposure_title,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'participated_in_patient_care' ->> 'df_value')::text AS participated_in_patient_care,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'participated_in_burial' ->> 'df_value')::text AS participated_in_burial,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'one_week_exposure_title' ->> 'df_value')::text AS one_week_exposure_title,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_fever' ->> 'df_value')::text AS symptom_fever,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_diarrhoea' ->> 'df_value')::text AS symptom_diarrhoea,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'generic_headache' ->> 'df_value')::text AS generic_headache,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_joint_muscle_pain' ->> 'df_value')::text AS symptom_joint_muscle_pain,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_bone_pain' ->> 'df_value')::text AS symptom_bone_pain,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_unexplained_bruising' ->> 'df_value')::text AS symptom_unexplained_bruising,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_unexplained_body_weakness' ->> 'df_value')::text AS symptom_unexplained_body_weakness,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_internal_external_bleeding' ->> 'df_value')::text AS symptom_internal_external_bleeding,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_sore_painfull_throat' ->> 'df_value')::text AS symptom_sore_painfull_throat,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_cough_vomiting' ->> 'df_value')::text AS symptom_cough_vomiting,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_common_cold' ->> 'df_value')::text AS symptom_common_cold,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors' -> 0 -> 'DFields' -> 'values' -> 'symptom_weight_loss' ->> 'df_value')::text AS symptom_weight_loss,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'date_of_arrival'->> 'df_value')::text AS date_of_arrival,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'point_of_entry' ->> 'df_value')::text AS point_of_entry,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'point_of_entry_other' ->> 'df_value')::text AS point_of_entry_other,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'country_of_departure' ->> 'df_value')::text AS country_of_departure,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'town_of_departure'->> 'df_value')::text AS town_of_departure,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'mode_of_transport'->> 'df_value')::text AS mode_of_transport,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'registration_number_of_conveyance' ->> 'df_value')::text AS registration_number_of_conveyance,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'number_of_seat' ->> 'df_value')::text AS number_of_seat,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'purpose_of_travel'->> 'df_value')::text AS purpose_of_travel,
    (doc -> 'DForms' -> 'traveller_marburg_travel_information' -> 0 -> 'DFields' -> 'values' -> 'expected_durartion_of_stay'->> 'df_value')::text AS expected_duration_of_stay,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors_travel' -> 0 -> 'DFields' -> 'values' -> 'travelled_to_country_with_confirmed_cases' ->> 'df_value')::text AS travelled_to_country_with_confirmed_cases,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors_travel' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS travel_country,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors_travel' -> 0 -> 'DFields' -> 'values' -> 'city_town_village'->> 'df_value')::text AS travel_city_town_village,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors_travel' -> 0 -> 'DFields' -> 'values' -> 'date_of_visit_start'->> 'df_value')::text AS travel_date_of_visit_start,
    (doc -> 'DForms' -> 'traveller_marburg_risk_factors_travel' -> 0 -> 'DFields' -> 'values' -> 'date_of_visit_end'->> 'df_value')::text AS travel_date_of_visit_end,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS local_county,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'city_town_village' ->> 'df_value')::text AS local_city_town_village,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'sublocation_estate'->> 'df_value')::text AS local_sublocation_estate,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'house_hotel' ->> 'df_value')::text AS local_house_hotel,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'postal_address'->> 'df_value')::text AS local_postal_address,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'phone_number_of_contact'->> 'df_value')::text AS local_phone_number_of_contact,
    (doc -> 'DForms' -> 'traveller_marburg_local_residence_and_contact' -> 0 -> 'DFields' -> 'values' -> 'phone_number_used_while_in_kenya' ->> 'df_value')::text AS local_phone_number_used_while_in_kenya
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc ->> 'mform_id')::text = 'f6fb6040-8154-11ef-91dc-dfc2afa38337'
AND
    (doc ->> 'ident') IS NOT NULL
