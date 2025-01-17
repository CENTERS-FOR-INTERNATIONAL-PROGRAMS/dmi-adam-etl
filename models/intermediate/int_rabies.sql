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
    CASE WHEN exposure_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(exposure_date, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN exposure_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(exposure_date, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
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
    CASE WHEN date_of_birth ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_birth, 'DD/MM/YYYY')::date ELSE NULL END AS date_of_birth,
    case_is_pregnant::text AS case_is_pregnant,
    phone_number::text AS phone_number,
    marital_status::text AS marital_status,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    level_of_education::text AS level_of_education,
    case_has_insurance_cover::text AS case_has_insurance_cover,
    country::text AS country,
    county::text AS county,
    subcounty::text AS subcounty,
    ward::text AS ward,
    town_village_camp::text AS town_village_camp,
    landmark::text AS landmark,
    title_residence::text AS title_residence,
    type_of_residence::text AS type_of_residence,
    title_parent_guardian::text AS title_parent_guardian,
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
    exposure_type_rabies::text AS exposure_type_rabies,
    exposure_date::text AS exposure_date,
    exposure_rabies_injury::text AS exposure_rabies_injury,
    exposure_rabies_injury_site::text AS exposure_rabies_injury_site,
    animal_species::text AS animal_species,
    animal_species_other::text AS animal_species_other,
    animal_vaccinated::text AS animal_vaccinated,
    animal_rabies_suspect::text AS animal_rabies_suspect,
    animal_observed::text AS animal_observed,
    animal_dead::text AS animal_dead,
    animal_dead_days::text AS animal_dead_days,
    repondent::text AS repondent,
    repondent_family_name::text AS repondent_family_name,
    repondent_given_name::text AS repondent_given_name,
    repondent_phone_number::text AS repondent_phone_number,
    repondent_address::text AS repondent_address,
    repondent_relationship::text AS repondent_relationship,
    repondent_relationship_other::text AS repondent_relationship_other,
    vaccinated::text AS vaccinated,
    vaccine_name::text AS vaccine_name,
    vaccine_doses::text AS vaccine_doses,
    health_facility::text AS health_facility,
    vaccination_date::text AS vaccination_date,
    case_seen_at_facility::text AS case_seen_at_facility,
    date_first_seen_at_facility::text AS date_first_seen_at_facility,
    admitted::text AS admitted,
    health_facility_name::text AS health_facility_name,
    admission_date::text AS admission_date,
    inpatient_number::text AS inpatient_number,
    discharge_date::text AS discharge_date,
    patient_status::text AS patient_status
FROM {{ ref('stg_rabies') }}
WHERE df_complete::text = 'true'
