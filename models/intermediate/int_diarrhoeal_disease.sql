SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    given || ' ' || family || ' (' || type_of_case || ')' AS contact_tree_label,
    mform_id::text AS mform_id,
    mform_event::text AS mform_event,
    event_ident::text AS event_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    (CASE WHEN df_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS completed,
    (CASE WHEN df_parent_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS parent_completed,
    created_role::text AS created_role,
    modified_role::text AS modified_role,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    syndrome::text AS syndrome,
    disease::text AS disease,
    CASE WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_onset, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_onset, 'DD/MM/YYYY'), 'YYYY"-"IW') ELSE NULL END AS epi_week,
    epid::text AS epid,
    date_of_investigation::text AS date_of_investigation,
    location_of_investigation::text AS location_of_investigation,
    location_of_investigation_other::text AS location_of_investigation_other,
    type_of_case::text AS type_of_case,
    given::text AS given_name,
    family::text AS family_name,
    sex::text AS sex,
    CASE 
        WHEN age_years ~ '^[0-9]+(\.[0-9]+)?$' THEN age_years::float
        ELSE NULL
    END AS age_years,
    CASE 
        WHEN age_months ~ '^[0-9]+(\.[0-9]+)?$' THEN age_months::float
        ELSE NULL
    END AS age_months,
    CASE 
        WHEN age_days ~ '^[0-9]+(\.[0-9]+)?$' THEN age_days::float
        ELSE NULL
    END AS age_days,
    CASE
        WHEN NOT (age_years ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN age_years::float < 2 THEN '0-2 yrs'
        WHEN age_years::float BETWEEN 2 AND 5 THEN '2-5 yrs'
        WHEN age_years::float BETWEEN 5 AND 16 THEN '5-16 yrs'
        WHEN age_years::float > 16 THEN '16+ yrs'
        ELSE 'Unknown'
    END AS age_group,
    case_is_pregnant::text AS case_is_pregnant,
    marital_status::text AS marital_status,
    case_has_insurance_cover::text AS case_has_insurance_cover,
    country::text AS country,
    county::text AS county,
    subcounty::text AS subcounty,
    ward::text AS ward,
    type_of_residence::text AS type_of_residence,
    town_village_camp::text AS town_village_camp,
    level_of_education::text AS level_of_education,
    phone_number::text AS phone_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    title_residence::text AS title_residence,
    landmark::text AS landmark,
    title_parent_guradian::text AS title_parent_guradian,
    guardian_family_name::text AS guardian_family_name,
    guardian_given_name::text AS guardian_given_name,
    guardian_phone_number::text AS guardian_phone_number,
    title_respondent::text AS title_respondent,
    respondent::text AS respondent,
    respondent_family_name::text AS respondent_family_name,
    respondent_given_name::text AS respondent_given_name,
    respondent_address::text AS respondent_address,
    respondent_phone_number::text AS respondent_phone_number,
    respondent_relationship::text AS respondent_relationship,
    record_exposure::text AS record_exposure,
    exposure_type::text AS exposure_type,
    other_water_source::text AS other_water_source,
    other_water_source_treated::text AS other_water_source_treated,
    other_water_source_treatment_method::text AS other_water_source_treatment_method,
    other_water_source_treatment_method_other::text AS other_water_source_treatment_method_other,
    food_name::text AS food_name,
    food_consumption_date::text AS food_consumption_date,
    food_source::text AS food_source,
    food_participants_count::text AS food_participants_count,
    food_affected_participants_count::text AS food_affected_participants_count,
    water_sources::text AS water_sources,
    water_sources_other::text AS water_sources_other,
    toilet_types::text AS toilet_types,
    toilet_types_other::text AS toilet_types_other,
    food_consumed::text AS food_consumed,
    external_food_consumed::text AS external_food_consumed,
    name_of_vendor_or_place::text AS name_of_vendor_or_place,
    method_of_waste_disporsal::text AS method_of_waste_disporsal,
    method_of_waste_disporsal_other::text AS method_of_waste_disporsal_other,
    waste_disporsal_shared::text AS waste_disporsal_shared,
    handwashing_moments::text AS handwashing_moments,
    handwashing_moments_other::text AS handwashing_moments_other,
    handwashing_with_soap::text AS handwashing_with_soap,
    contact_with_case::text AS contact_with_case,
    type_of_contact::text AS type_of_contact,
    place_of_interaction::text AS place_of_interaction,
    attended_social_gathering::text AS attended_social_gathering,
    location_of_social_gathering::text AS location_of_social_gathering,
    date_of_social_gathering::text AS date_of_social_gathering,
    type_of_social_event::text AS type_of_social_event,
    type_of_social_event_other::text AS type_of_social_event_other,
    description_of_social_event::text AS description_of_social_event,
    case_travelled::text AS case_travelled,
    travel_origin_country::text AS travel_origin_country,
    travel_origin_city::text AS travel_origin_city,
    travel_departure_date::text AS travel_departure_date,
    travel_destination_country::text AS travel_destination_country,
    destination_county::text AS destination_county,
    destination_subcounty::text AS destination_subcounty,
    travel_destination_city::text AS travel_destination_city,
    travel_arrival_date::text AS travel_arrival_date,
    level_of_hydration::text AS level_of_hydration,
    antibiotics_taken::text AS antibiotics_taken,
    antibiotic_name::text AS antibiotic_name,
    title_clinical_care::text AS title_clinical_care,
    type_of_care::text AS type_of_care,
    health_facility_mfl_number::text AS health_facility_mfl_number,
    health_facility_name::text AS health_facility_name,
    date_of_admission::text AS date_of_admission,
    inpatient_number::text AS inpatient_number,
    outpatient_number::text AS outpatient_number,
    outcome_of_patient::text AS outcome_of_patient,
    date_of_death::text AS date_of_death,
    place_of_death::text AS place_of_death,
    status_of_patient::text AS status_of_patient,
    date_of_discharge::text AS date_of_discharge,
    rdt_done::text AS rdt_done,
    rdt_results::text AS rdt_results,
    name_of_laboratory::text AS name_of_laboratory,
    date_of_sample_collection::text AS date_of_sample_collection,
    were_samples_collected::text AS were_samples_collected,
    laboratory_samples_collected::text AS laboratory_samples_collected,
    laboratory_samples_collected_others::text AS laboratory_samples_collected_others,
    vaccinated::text AS vaccinated,
    vaccine_doses::text AS vaccine_doses,
    vaccination_date::text AS vaccination_date,
    date_of_onset::text AS date_of_onset,
    symptoms::text AS symptoms,
    symptoms_other::text AS symptoms_other,
    culture_done::text AS culture_done,
    date_of_culture_test::text AS date_of_culture_test,
    title_laboratory_facility::text AS title_laboratory_facility,
    title_results::text AS title_results,
    date_of_culture_result::text AS date_of_culture_result,
    culture_result_diarrhoeal::text AS culture_result_diarrhoeal
FROM {{ ref('stg_diarrhoeal_disease') }}
WHERE df_complete::text = 'true'
