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
      {'columns': ['total']},
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

WITH RECURSIVE

root_node AS (
    SELECT
        'a52f1b40-89b1-22df-a632-9dcf84b7c053'::text AS id,
        ''::text AS parent_id,
        0 AS hierarchy_level,
        'ROOT'::text AS contact_tree_label,
        NULL AS mform_id,
        NULL AS mform_event,
        NULL AS event_id,
        NULL AS form_id,
        NULL AS case_unique_id,
        'Draft'::text AS completed,
        NULL AS parent_completed,
        NULL AS created_role,
        NULL AS modified_role,
        NULL AS created_username,
        NULL AS created_timestamp,
        NULL AS modified_username,
        NULL AS modified_timestamp,
        NULL AS location_accuracy,
        NULL AS location_latitude,
        NULL AS location_longitude,
        'Diarrhoeal Disease'::text AS syndrome,
        NULL AS disease,
        null::date AS case_date,
        NULL AS epi_week,
        NULL AS epid,
        NULL AS date_of_investigation,
        NULL AS location_of_investigation,
        NULL AS location_of_investigation_other,
        NULL AS type_of_case,
        NULL AS given_name,
        NULL AS family_name,
        NULL AS sex,
        null::double precision AS age_years,
        null::double precision AS age_months,
        null::double precision AS age_days,
        NULL AS age_group,
        NULL AS case_is_pregnant,
        NULL AS marital_status,
        NULL AS case_has_insurance_cover,
        NULL AS country,
        NULL AS county,
        NULL AS subcounty,
        NULL AS ward,
        NULL AS type_of_residence,
        NULL AS town_village_camp,
        NULL AS level_of_education,
        NULL AS phone_number,
        NULL AS occupation,
        NULL AS occupation_other,
        NULL AS title_residence,
        NULL AS landmark,
        NULL AS title_parent_guradian,
        NULL AS guardian_family_name,
        NULL AS guardian_given_name,
        NULL AS guardian_phone_number,
        NULL AS title_respondent,
        NULL AS respondent,
        NULL AS respondent_family_name,
        NULL AS respondent_given_name,
        NULL AS respondent_address,
        NULL AS respondent_phone_number,
        NULL AS respondent_relationship,
        NULL AS record_exposure,
        NULL AS exposure_type,
        NULL AS other_water_source,
        NULL AS other_water_source_treated,
        NULL AS other_water_source_treatment_method,
        NULL AS other_water_source_treatment_method_other,
        NULL AS food_name,
        NULL AS food_consumption_date,
        NULL AS food_source,
        NULL AS food_participants_count,
        NULL AS food_affected_participants_count,
        NULL AS water_sources,
        NULL AS water_sources_other,
        NULL AS toilet_types,
        NULL AS toilet_types_other,
        NULL AS food_consumed,
        NULL AS external_food_consumed,
        NULL AS name_of_vendor_or_place,
        NULL AS method_of_waste_disporsal,
        NULL AS method_of_waste_disporsal_other,
        NULL AS waste_disporsal_shared,
        NULL AS handwashing_moments,
        NULL AS handwashing_moments_other,
        NULL AS handwashing_with_soap,
        NULL AS contact_with_case,
        NULL AS type_of_contact,
        NULL AS place_of_interaction,
        NULL AS attended_social_gathering,
        NULL AS location_of_social_gathering,
        NULL AS date_of_social_gathering,
        NULL AS type_of_social_event,
        NULL AS type_of_social_event_other,
        NULL AS description_of_social_event,
        NULL AS case_travelled,
        NULL AS travel_origin_country,
        NULL AS travel_origin_city,
        NULL AS travel_departure_date,
        NULL AS travel_destination_country,
        NULL AS destination_county,
        NULL AS destination_subcounty,
        NULL AS travel_destination_city,
        NULL AS travel_arrival_date,
        NULL AS level_of_hydration,
        NULL AS antibiotics_taken,
        NULL AS antibiotic_name,
        NULL AS title_clinical_care,
        NULL AS type_of_care,
        NULL AS health_facility_mfl_number,
        NULL AS health_facility_name,
        NULL AS date_of_admission,
        NULL AS inpatient_number,
        NULL AS outpatient_number,
        NULL AS outcome_of_patient,
        NULL AS date_of_death,
        NULL AS place_of_death,
        NULL AS status_of_patient,
        NULL AS date_of_discharge,
        NULL AS rdt_done,
        NULL AS rdt_results,
        NULL AS name_of_laboratory,
        NULL AS date_of_sample_collection,
        NULL AS were_samples_collected,
        NULL AS laboratory_samples_collected,
        NULL AS laboratory_samples_collected_others,
        NULL AS vaccinated,
        NULL AS vaccine_doses,
        NULL AS vaccination_date,
        NULL AS date_of_onset,
        NULL AS symptoms,
        NULL AS symptoms_other,
        NULL AS culture_done,
        NULL AS date_of_culture_test,
        NULL AS title_laboratory_facility,
        NULL AS title_results,
        NULL AS date_of_culture_result,
        NULL AS culture_result_diarrhoeal,
        0::integer AS total,
        0::integer AS suspected,
        0::integer AS tested,
        0::integer AS confirmed,
        0::integer AS admitted,
        0::integer AS discharged,
        0::integer AS died,
        0::integer AS probable,
        0::integer AS contact,
        current_date AS load_date
),

hierarchy AS (
    SELECT * FROM root_node
    
    UNION ALL

    SELECT 
        c.id,
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c053'
            ELSE c.parent_id
        END AS parent_id,
        h.hierarchy_level + 1 AS hierarchy_level,
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
        c.given_name,
        c.family_name,
        c.sex,
        c.age_years,
        c.age_months,
        c.age_days,
        c.age_group,
        c.case_is_pregnant,
        c.marital_status,
        c.case_has_insurance_cover,
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.type_of_residence,
        c.town_village_camp,
        c.level_of_education,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.title_residence,
        c.landmark,
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
        c.record_exposure,
        c.exposure_type,
        c.other_water_source,
        c.other_water_source_treated,
        c.other_water_source_treatment_method,
        c.other_water_source_treatment_method_other,
        c.food_name,
        c.food_consumption_date,
        c.food_source,
        c.food_participants_count,
        c.food_affected_participants_count,
        c.water_sources,
        c.water_sources_other,
        c.toilet_types,
        c.toilet_types_other,
        c.food_consumed,
        c.external_food_consumed,
        c.name_of_vendor_or_place,
        c.method_of_waste_disporsal,
        c.method_of_waste_disporsal_other,
        c.waste_disporsal_shared,
        c.handwashing_moments,
        c.handwashing_moments_other,
        c.handwashing_with_soap,
        c.contact_with_case,
        c.type_of_contact,
        c.place_of_interaction,
        c.attended_social_gathering,
        c.location_of_social_gathering,
        c.date_of_social_gathering,
        c.type_of_social_event,
        c.type_of_social_event_other,
        c.description_of_social_event,
        c.case_travelled,
        c.travel_origin_country,
        c.travel_origin_city,
        c.travel_departure_date,
        c.travel_destination_country,
        c.destination_county,
        c.destination_subcounty,
        c.travel_destination_city,
        c.travel_arrival_date,
        c.level_of_hydration,
        c.antibiotics_taken,
        c.antibiotic_name,
        c.title_clinical_care,
        c.type_of_care,
        c.health_facility_mfl_number,
        c.health_facility_name,
        c.date_of_admission,
        c.inpatient_number,
        c.outpatient_number,
        c.outcome_of_patient,
        c.date_of_death,
        c.place_of_death,
        c.status_of_patient,
        c.date_of_discharge,
        c.rdt_done,
        c.rdt_results,
        c.name_of_laboratory,
        c.date_of_sample_collection,
        c.were_samples_collected,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_others,
        c.vaccinated,
        c.vaccine_doses,
        c.vaccination_date,
        c.date_of_onset,
        c.symptoms,
        c.symptoms_other,
        c.culture_done,
        c.date_of_culture_test,
        c.title_laboratory_facility,
        c.title_results,
        c.date_of_culture_result,
        c.culture_result_diarrhoeal,
        (1)::integer AS total,
        (CASE WHEN c.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN c.culture_done = 'Yes' THEN 1 WHEN c.rdt_done = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.culture_result_diarrhoeal = 'Positive' THEN 1 WHEN c.rdt_results = 'Positive' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_diarrhoeal_disease') }} c
    INNER JOIN hierarchy h ON 
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c053'
            ELSE c.parent_id
        END = h.id
    WHERE NOT (c.id = 'a52f1b40-89b1-22df-a632-9dcf84b7c053')
),

first_level_orphans AS (
    SELECT 
        c.id,
        'a52f1b40-89b1-22df-a632-9dcf84b7c053' AS parent_id,
        1 AS hierarchy_level,
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
        c.given_name,
        c.family_name,
        c.sex,
        c.age_years,
        c.age_months,
        c.age_days,
        c.age_group,
        c.case_is_pregnant,
        c.marital_status,
        c.case_has_insurance_cover,
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.type_of_residence,
        c.town_village_camp,
        c.level_of_education,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.title_residence,
        c.landmark,
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
        c.record_exposure,
        c.exposure_type,
        c.other_water_source,
        c.other_water_source_treated,
        c.other_water_source_treatment_method,
        c.other_water_source_treatment_method_other,
        c.food_name,
        c.food_consumption_date,
        c.food_source,
        c.food_participants_count,
        c.food_affected_participants_count,
        c.water_sources,
        c.water_sources_other,
        c.toilet_types,
        c.toilet_types_other,
        c.food_consumed,
        c.external_food_consumed,
        c.name_of_vendor_or_place,
        c.method_of_waste_disporsal,
        c.method_of_waste_disporsal_other,
        c.waste_disporsal_shared,
        c.handwashing_moments,
        c.handwashing_moments_other,
        c.handwashing_with_soap,
        c.contact_with_case,
        c.type_of_contact,
        c.place_of_interaction,
        c.attended_social_gathering,
        c.location_of_social_gathering,
        c.date_of_social_gathering,
        c.type_of_social_event,
        c.type_of_social_event_other,
        c.description_of_social_event,
        c.case_travelled,
        c.travel_origin_country,
        c.travel_origin_city,
        c.travel_departure_date,
        c.travel_destination_country,
        c.destination_county,
        c.destination_subcounty,
        c.travel_destination_city,
        c.travel_arrival_date,
        c.level_of_hydration,
        c.antibiotics_taken,
        c.antibiotic_name,
        c.title_clinical_care,
        c.type_of_care,
        c.health_facility_mfl_number,
        c.health_facility_name,
        c.date_of_admission,
        c.inpatient_number,
        c.outpatient_number,
        c.outcome_of_patient,
        c.date_of_death,
        c.place_of_death,
        c.status_of_patient,
        c.date_of_discharge,
        c.rdt_done,
        c.rdt_results,
        c.name_of_laboratory,
        c.date_of_sample_collection,
        c.were_samples_collected,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_others,
        c.vaccinated,
        c.vaccine_doses,
        c.vaccination_date,
        c.date_of_onset,
        c.symptoms,
        c.symptoms_other,
        c.culture_done,
        c.date_of_culture_test,
        c.title_laboratory_facility,
        c.title_results,
        c.date_of_culture_result,
        c.culture_result_diarrhoeal,
        (1)::integer AS total,
        (CASE WHEN c.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN c.culture_done = 'Yes' THEN 1 WHEN c.rdt_done = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.culture_result_diarrhoeal = 'Positive' THEN 1 WHEN c.rdt_results = 'Positive' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_diarrhoeal_disease') }} c
    LEFT JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.id IS NULL 
      AND c.parent_id IS NOT NULL 
      AND c.parent_id != '' 
      AND c.id != 'a52f1b40-89b1-22df-a632-9dcf84b7c053'
),

combined_hierarchy AS (
    SELECT * FROM hierarchy
    UNION ALL
    SELECT * FROM first_level_orphans
)

SELECT
    h.id,
    h.parent_id,
    h.hierarchy_level,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM {{ ref('int_diarrhoeal_disease') }} child 
            WHERE child.parent_id = h.id
        ) THEN true
        ELSE false
    END as has_children,
    h.contact_tree_label,
    h.mform_id,
    h.mform_event,
    h.event_id,
    h.form_id,
    h.case_unique_id,
    h.completed,
    h.parent_completed,
    h.created_role,
    h.modified_role,
    h.created_username,
    h.created_timestamp,
    h.modified_username,
    h.modified_timestamp,
    h.location_accuracy,
    h.location_latitude,
    h.location_longitude,
    'Diarrhoeal Disease'::text AS syndrome,
    h.disease,
    h.case_date,
    h.epi_week,
    h.epid,
    h.date_of_investigation,
    h.location_of_investigation,
    h.location_of_investigation_other,
    CASE WHEN h.type_of_case IS NOT NULL THEN h.type_of_case ELSE 'Unknown' END AS type_of_case,
    h.given_name,
    h.family_name,
    h.sex,
    h.age_years,
    h.age_months,
    h.age_days,
    h.age_group,
    h.case_is_pregnant,
    h.marital_status,
    h.case_has_insurance_cover,
    h.country,
    h.county,
    h.subcounty,
    h.ward,
    h.type_of_residence,
    h.town_village_camp,
    h.level_of_education,
    h.phone_number,
    h.occupation,
    h.occupation_other,
    h.title_residence,
    h.landmark,
    h.title_parent_guradian,
    h.guardian_family_name,
    h.guardian_given_name,
    h.guardian_phone_number,
    h.title_respondent,
    h.respondent,
    h.respondent_family_name,
    h.respondent_given_name,
    h.respondent_address,
    h.respondent_phone_number,
    h.respondent_relationship,
    h.record_exposure,
    h.exposure_type,
    h.other_water_source,
    h.other_water_source_treated,
    h.other_water_source_treatment_method,
    h.other_water_source_treatment_method_other,
    h.food_name,
    h.food_consumption_date,
    h.food_source,
    h.food_participants_count,
    h.food_affected_participants_count,
    h.water_sources,
    h.water_sources_other,
    h.toilet_types,
    h.toilet_types_other,
    h.food_consumed,
    h.external_food_consumed,
    h.name_of_vendor_or_place,
    h.method_of_waste_disporsal,
    h.method_of_waste_disporsal_other,
    h.waste_disporsal_shared,
    h.handwashing_moments,
    h.handwashing_moments_other,
    h.handwashing_with_soap,
    h.contact_with_case,
    h.type_of_contact,
    h.place_of_interaction,
    h.attended_social_gathering,
    h.location_of_social_gathering,
    h.date_of_social_gathering,
    h.type_of_social_event,
    h.type_of_social_event_other,
    h.description_of_social_event,
    h.case_travelled,
    h.travel_origin_country,
    h.travel_origin_city,
    h.travel_departure_date,
    h.travel_destination_country,
    h.destination_county,
    h.destination_subcounty,
    h.travel_destination_city,
    h.travel_arrival_date,
    h.level_of_hydration,
    h.antibiotics_taken,
    h.antibiotic_name,
    h.title_clinical_care,
    h.type_of_care,
    h.health_facility_mfl_number,
    h.health_facility_name,
    h.date_of_admission,
    h.inpatient_number,
    h.outpatient_number,
    h.outcome_of_patient,
    h.date_of_death,
    h.place_of_death,
    h.status_of_patient,
    h.date_of_discharge,
    h.rdt_done,
    h.rdt_results,
    h.name_of_laboratory,
    h.date_of_sample_collection,
    h.were_samples_collected,
    h.laboratory_samples_collected,
    h.laboratory_samples_collected_others,
    h.vaccinated,
    h.vaccine_doses,
    h.vaccination_date,
    h.date_of_onset,
    h.symptoms,
    h.symptoms_other,
    h.culture_done,
    h.date_of_culture_test,
    h.title_laboratory_facility,
    h.title_results,
    h.date_of_culture_result,
    h.culture_result_diarrhoeal,
    h.total,
    h.suspected,
    h.tested,
    h.confirmed,
    h.admitted,
    h.discharged,
    h.died,
    h.probable,
    h.contact,
    h.load_date
FROM combined_hierarchy h
