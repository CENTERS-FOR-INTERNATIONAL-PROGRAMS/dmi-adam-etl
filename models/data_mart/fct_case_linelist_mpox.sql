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
      {'columns': ['hierarchy_level']},
      {'columns': ['has_children']}
    ]
)}}

WITH RECURSIVE hierarchy AS (
    -- Base case: root node
    SELECT 
        m.id,
        m.parent_id,
        1 AS hierarchy_level,
        m.contact_tree_label,
        m.mform_id,
        m.mform_event,
        m.event_id,
        m.form_id,
        m.case_unique_id,
        m.completed,
        m.parent_completed,
        m.created_role,
        m.modified_role,
        m.created_username,
        m.created_timestamp,
        m.modified_username,
        m.modified_timestamp,
        m.location_accuracy,
        m.location_latitude,
        m.location_longitude,
        m.syndrome,
        m.disease,
        m.case_date,
        m.epi_week,
        m.epid,
        m.date_of_investigation,
        m.location_of_investigation,
        m.location_of_investigation_other,
        m.type_of_case,
        m.family_name,
        m.given_name,
        m.sex,
        m.age_years,
        m.age_months,
        m.age_days,
        m.age_group,
        m.date_of_birth,
        m.pregnant,
        m.phone_number,
        m.occupation,
        m.occupation_other,
        m.title_residence,
        m.type_of_residence,
        m.country,
        m.county,
        m.subcounty,
        m.ward,
        m.town_village_camp,
        m.landmark,
        m.marital_status,
        m.level_of_education,
        m.case_has_insurance_cover,
        m.title_parent_guradian,
        m.guardian_family_name,
        m.guardian_given_name,
        m.guardian_phone_number,
        m.title_respondent,
        m.respondent,
        m.respondent_family_name,
        m.respondent_given_name,
        m.respondent_address,
        m.respondent_phone_number,
        m.respondent_relationship,
        m.symptoms,
        m.symptoms_other,
        m.fever_temperature,
        m.locations_of_lymphadenopathy,
        m.date_of_onset,
        m.start_location_of_rash,
        m.start_location_of_rash_other,
        m.spread_location_of_rash,
        m.spread_location_of_rash_other,
        m.number_of_lesions,
        m.development_of_rash,
        m.distribution_of_rash,
        m.history_of_immunosuppressive_conditions,
        m.hiv_positive,
        m.date_of_hiv_diagnosis,
        m.patient_on_art,
        m.history_of_skin_disease,
        m.recently_given_birth,
        m.baby_is_symptomatic,
        m.case_is_breatfeeding,
        m.case_was_breastfeeding_three_weeks_prior_to_illness,
        m.title_clinical_care,
        m.type_of_care,
        m.case_is_isolated,
        m.health_facility_name,
        m.reason_for_hospitalization,
        m.reason_for_hospitalization_other,
        m.date_of_admission,
        m.inpatient_number,
        m.date_first_seen_at_facility,
        m.outpatient_number,
        m.treatment_given,
        m.treatment_given_other,
        m.outcome_of_patient,
        m.status_of_patient,
        m.date_of_discharge,
        m.date_of_death,
        m.place_of_death,
        m.food_name,
        m.food_source,
        m.food_consumption_date,
        m.food_participants_count,
        m.food_affected_participants_count,
        m.water_sources,
        m.water_sources_other,
        m.toilet_types,
        m.toilet_types_other,
        m.travel_origin_country,
        m.travel_origin_city,
        m.travel_departure_date,
        m.travel_destination_country,
        m.travel_destination_city,
        m.travel_arrival_date,
        m.animal_exposure_location,
        m.type_of_exposure,
        m.type_of_contact,
        m.contact_was_symptomatic,
        m.date_of_contact,
        m.frequency_of_contact,
        m.duration_of_contact,
        m.location_of_contact,
        m.location_of_contact_other,
        m.relationship_of_contact,
        m.relationship_of_contact_other,
        m.title_ppe,
        m.case_was_wearing_ppe,
        m.ppe_items_worn,
        m.ppe_breach,
        m.hand_hygiene_before_wearing_ppe,
        m.hand_hygiene_after_wearing_ppe,
        m.animal_species,
        m.animal_species_other,
        m.animal_exposure,
        m.animal_exposure_other,
        m.date_of_exposure,
        m.case_was_ill,
        m.description_of_event,
        m.location_of_event,
        m.date_of_event,
        m.description_of_illness,
        m.type_of_direct_contact,
        m.type_of_direct_contact_other,
        m.samples_were_collected,
        m.samples_collected,
        m.samples_collected_other,
        m.date_of_sample_collection,
        m.tests,
        m.test_other,
        m.result_of_laboratory_test,
        m.date_of_laboratory_testing_results,
        m.facility_name,
        m.facility_name_other,
        m.clade_of_mpox,
        m.day_of_follow_up,
        m.date_of_follow_up,
        m.contact_is_symptomatic,
        m.smallpox_mpox_vaccine_received,
        m.number_of_doses,
        m.date_of_first_dose,
        m.date_of_last_dose,
        m.brand_of_vaccine,
        m.brand_of_vaccine_other,
        m.route_of_admission,
        m.route_of_admission_other,
        m.reason_for_vaccination,
        (CASE WHEN m.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN m.samples_were_collected = 'Yes' THEN 1 WHEN m.tests IS NOT NULL THEN 1 WHEN m.test_other IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN m.type_of_case = 'Confirmed' THEN 1 WHEN m.result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN m.outcome_of_patient = 'Admitted' THEN 1 WHEN m.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN m.outcome_of_patient = 'Discharged' THEN 1 WHEN m.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN m.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN m.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN m.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_mpox') }} m
    WHERE parent_id = '' OR parent_id IS NULL

    UNION ALL

    -- Recursive case: child nodes
    SELECT 
        c.id,
        c.parent_id,
        h.hierarchy_level + 1,
        c.contact_tree_label,
        c.mform_id,
        c.mform_event,
        c.event_id,
        c.form_id,
        c.case_unique_id,
        c.completed,
        c.parent_completed,
        c.created_role,
        c.modified_role,
        c.created_username,
        c.created_timestamp,
        c.modified_username,
        c.modified_timestamp,
        c.location_accuracy,
        c.location_latitude,
        c.location_longitude,
        c.syndrome,
        c.disease,
        c.case_date,
        c.epi_week,
        c.epid,
        c.date_of_investigation,
        c.location_of_investigation,
        c.location_of_investigation_other,
        c.type_of_case,
        c.family_name,
        c.given_name,
        c.sex,
        c.age_years,
        c.age_months,
        c.age_days,
        c.age_group,
        c.date_of_birth,
        c.pregnant,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.title_residence,
        c.type_of_residence,
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.town_village_camp,
        c.landmark,
        c.marital_status,
        c.level_of_education,
        c.case_has_insurance_cover,
        c.title_parent_guradian,
        c.guardian_family_name,
        c.guardian_given_name,
        c.guardian_phone_number,
        c.title_respondent,
        c.respondent,
        c.respondent_family_name,
        c.respondent_given_name,
        c.respondent_address,
        c.respondent_phone_number,
        c.respondent_relationship,
        c.symptoms,
        c.symptoms_other,
        c.fever_temperature,
        c.locations_of_lymphadenopathy,
        c.date_of_onset,
        c.start_location_of_rash,
        c.start_location_of_rash_other,
        c.spread_location_of_rash,
        c.spread_location_of_rash_other,
        c.number_of_lesions,
        c.development_of_rash,
        c.distribution_of_rash,
        c.history_of_immunosuppressive_conditions,
        c.hiv_positive,
        c.date_of_hiv_diagnosis,
        c.patient_on_art,
        c.history_of_skin_disease,
        c.recently_given_birth,
        c.baby_is_symptomatic,
        c.case_is_breatfeeding,
        c.case_was_breastfeeding_three_weeks_prior_to_illness,
        c.title_clinical_care,
        c.type_of_care,
        c.case_is_isolated,
        c.health_facility_name,
        c.reason_for_hospitalization,
        c.reason_for_hospitalization_other,
        c.date_of_admission,
        c.inpatient_number,
        c.date_first_seen_at_facility,
        c.outpatient_number,
        c.treatment_given,
        c.treatment_given_other,
        c.outcome_of_patient,
        c.status_of_patient,
        c.date_of_discharge,
        c.date_of_death,
        c.place_of_death,
        c.food_name,
        c.food_source,
        c.food_consumption_date,
        c.food_participants_count,
        c.food_affected_participants_count,
        c.water_sources,
        c.water_sources_other,
        c.toilet_types,
        c.toilet_types_other,
        c.travel_origin_country,
        c.travel_origin_city,
        c.travel_departure_date,
        c.travel_destination_country,
        c.travel_destination_city,
        c.travel_arrival_date,
        c.animal_exposure_location,
        c.type_of_exposure,
        c.type_of_contact,
        c.contact_was_symptomatic,
        c.date_of_contact,
        c.frequency_of_contact,
        c.duration_of_contact,
        c.location_of_contact,
        c.location_of_contact_other,
        c.relationship_of_contact,
        c.relationship_of_contact_other,
        c.title_ppe,
        c.case_was_wearing_ppe,
        c.ppe_items_worn,
        c.ppe_breach,
        c.hand_hygiene_before_wearing_ppe,
        c.hand_hygiene_after_wearing_ppe,
        c.animal_species,
        c.animal_species_other,
        c.animal_exposure,
        c.animal_exposure_other,
        c.date_of_exposure,
        c.case_was_ill,
        c.description_of_event,
        c.location_of_event,
        c.date_of_event,
        c.description_of_illness,
        c.type_of_direct_contact,
        c.type_of_direct_contact_other,
        c.samples_were_collected,
        c.samples_collected,
        c.samples_collected_other,
        c.date_of_sample_collection,
        c.tests,
        c.test_other,
        c.result_of_laboratory_test,
        c.date_of_laboratory_testing_results,
        c.facility_name,
        c.facility_name_other,
        c.clade_of_mpox,
        c.day_of_follow_up,
        c.date_of_follow_up,
        c.contact_is_symptomatic,
        c.smallpox_mpox_vaccine_received,
        c.number_of_doses,
        c.date_of_first_dose,
        c.date_of_last_dose,
        c.brand_of_vaccine,
        c.brand_of_vaccine_other,
        c.route_of_admission,
        c.route_of_admission_other,
        c.reason_for_vaccination,
        (CASE WHEN c.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN c.samples_were_collected = 'Yes' THEN 1 WHEN c.tests IS NOT NULL THEN 1 WHEN c.test_other IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.type_of_case = 'Confirmed' THEN 1 WHEN c.result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.outcome_of_patient = 'Admitted' THEN 1 WHEN c.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.outcome_of_patient = 'Discharged' THEN 1 WHEN c.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_mpox') }} c
    INNER JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.hierarchy_level < 3  -- Limit to 3 levels
),

-- Add row number to handle the root node insert
numbered_hierarchy AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY hierarchy_level, id) as rn,
        h.*,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('int_mpox') }} child 
                WHERE child.parent_id = h.id
            ) THEN true
            ELSE false
        END as has_children
    FROM hierarchy h
)

-- Combine root record with hierarchy
SELECT
    CASE 
        WHEN rn = 1 THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c051'
        ELSE id 
    END AS id,
    CASE 
        WHEN rn = 1 THEN ''
        WHEN parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c051'
        ELSE parent_id 
    END AS parent_id,
    hierarchy_level,
    has_children,
    'Mpox' AS contact_tree_label,
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
    'Monkey Pox' AS disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    location_of_investigation,
    location_of_investigation_other,
    type_of_case,
    family_name,
    given_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    date_of_birth,
    pregnant,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    type_of_residence,
    country,
    county,
    subcounty,
    ward,
    town_village_camp,
    landmark,
    marital_status,
    level_of_education,
    case_has_insurance_cover,
    title_parent_guradian,
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
    symptoms,
    symptoms_other,
    fever_temperature,
    locations_of_lymphadenopathy,
    date_of_onset,
    start_location_of_rash,
    start_location_of_rash_other,
    spread_location_of_rash,
    spread_location_of_rash_other,
    number_of_lesions,
    development_of_rash,
    distribution_of_rash,
    history_of_immunosuppressive_conditions,
    hiv_positive,
    date_of_hiv_diagnosis,
    patient_on_art,
    history_of_skin_disease,
    recently_given_birth,
    baby_is_symptomatic,
    case_is_breatfeeding,
    case_was_breastfeeding_three_weeks_prior_to_illness,
    title_clinical_care,
    type_of_care,
    case_is_isolated,
    health_facility_name,
    reason_for_hospitalization,
    reason_for_hospitalization_other,
    date_of_admission,
    inpatient_number,
    date_first_seen_at_facility,
    outpatient_number,
    treatment_given,
    treatment_given_other,
    outcome_of_patient,
    status_of_patient,
    date_of_discharge,
    date_of_death,
    place_of_death,
    food_name,
    food_source,
    food_consumption_date,
    food_participants_count,
    food_affected_participants_count,
    water_sources,
    water_sources_other,
    toilet_types,
    toilet_types_other,
    travel_origin_country,
    travel_origin_city,
    travel_departure_date,
    travel_destination_country,
    travel_destination_city,
    travel_arrival_date,
    animal_exposure_location,
    type_of_exposure,
    type_of_contact,
    contact_was_symptomatic,
    date_of_contact,
    frequency_of_contact,
    duration_of_contact,
    location_of_contact,
    location_of_contact_other,
    relationship_of_contact,
    relationship_of_contact_other,
    title_ppe,
    case_was_wearing_ppe,
    ppe_items_worn,
    ppe_breach,
    hand_hygiene_before_wearing_ppe,
    hand_hygiene_after_wearing_ppe,
    animal_species,
    animal_species_other,
    animal_exposure,
    animal_exposure_other,
    date_of_exposure,
    case_was_ill,
    description_of_event,
    location_of_event,
    date_of_event,
    description_of_illness,
    type_of_direct_contact,
    type_of_direct_contact_other,
    samples_were_collected,
    samples_collected,
    samples_collected_other,
    date_of_sample_collection,
    tests,
    test_other,
    result_of_laboratory_test,
    date_of_laboratory_testing_results,
    facility_name,
    facility_name_other,
    clade_of_mpox,
    day_of_follow_up,
    date_of_follow_up,
    contact_is_symptomatic,
    smallpox_mpox_vaccine_received,
    number_of_doses,
    date_of_first_dose,
    date_of_last_dose,
    brand_of_vaccine,
    brand_of_vaccine_other,
    route_of_admission,
    route_of_admission_other,
    reason_for_vaccination,
    suspected,
    tested,
    confirmed,
    admitted,
    discharged,
    died,
    probable,
    contact,
    load_date
FROM numbered_hierarchy
WHERE hierarchy_level <= 3  -- Ensure we only get 3 levels