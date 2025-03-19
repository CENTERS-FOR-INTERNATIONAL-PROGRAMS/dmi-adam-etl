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
        'a52f1b40-89b1-22df-a632-9dcf84b7c057'::text AS id,
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
        NULL AS syndrome,
        'Neonatal Tetanus'::text AS disease,
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
        NULL::date AS date_of_birth,
        NULL AS case_is_pregnant,
        NULL AS marital_status,
        NULL AS phone_number,
        NULL AS occupation,
        NULL AS occupation_other,
        NULL AS level_of_education,
        NULL AS case_has_insurance_cover,
        NULL AS country,
        NULL AS county,
        NULL AS subcounty,
        NULL AS ward,
        NULL AS town_village_camp,
        NULL AS landmark,
        NULL AS title_residence,
        NULL AS type_of_residence,
        NULL AS title_parent_guardian,
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
        NULL AS delivery_location,
        NULL AS sterile_code_cutter,
        NULL AS cord_stump_treated,
        NULL AS baby_age_days,
        NULL AS baby_suckling_normally,
        NULL AS confirmed_nnt,
        NULL AS baby_treated_at_facility,
        NULL AS baby_mother_alive,
        NULL AS title_clinical_care,
        NULL AS type_of_care,
        NULL AS date_of_admission,
        NULL AS inpatient_number,
        NULL AS name_of_health_facility,
        NULL AS mfl_number_of_health_facility,
        NULL AS outcome_of_patient,
        NULL AS status_of_patient,
        NULL AS date_of_discharge,
        NULL AS date_of_death,
        NULL AS laboratory_samples_were_collected,
        NULL AS laboratory_samples_collected,
        NULL AS laboratory_samples_collected_other,
        NULL AS date_of_sample_collection,
        NULL AS result_of_laboratory_test,
        NULL AS date_of_laboratory_results,
        NULL AS title_laboratory_facility,
        NULL AS name_of_laboratory_facility,
        NULL AS mfl_number_of_laboratory_facility,
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
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c057'
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
        c.date_of_birth,
        c.case_is_pregnant,
        c.marital_status,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.level_of_education,
        c.case_has_insurance_cover,
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.town_village_camp,
        c.landmark,
        c.title_residence,
        c.type_of_residence,
        c.title_parent_guardian,
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
        c.delivery_location,
        c.sterile_code_cutter,
        c.cord_stump_treated,
        c.baby_age_days,
        c.baby_suckling_normally,
        c.confirmed_nnt,
        c.baby_treated_at_facility,
        c.baby_mother_alive,
        c.title_clinical_care,
        c.type_of_care,
        c.date_of_admission,
        c.inpatient_number,
        c.name_of_health_facility,
        c.mfl_number_of_health_facility,
        c.outcome_of_patient,
        c.status_of_patient,
        c.date_of_discharge,
        c.date_of_death,
        c.laboratory_samples_were_collected,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_other,
        c.date_of_sample_collection,
        c.result_of_laboratory_test,
        c.date_of_laboratory_results,
        c.title_laboratory_facility,
        c.name_of_laboratory_facility,
        c.mfl_number_of_laboratory_facility,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (CASE WHEN c.laboratory_samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.confirmed_nnt = 'Yes' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 WHEN c.result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_neonatal_tetanus') }} c
    INNER JOIN hierarchy h ON 
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c057'
            ELSE c.parent_id
        END = h.id
    WHERE NOT (c.id = 'a52f1b40-89b1-22df-a632-9dcf84b7c057')
),

first_level_orphans AS (
    SELECT 
        c.id,
        'a52f1b40-89b1-22df-a632-9dcf84b7c057' AS parent_id,
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
        c.date_of_birth,
        c.case_is_pregnant,
        c.marital_status,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.level_of_education,
        c.case_has_insurance_cover,
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.town_village_camp,
        c.landmark,
        c.title_residence,
        c.type_of_residence,
        c.title_parent_guardian,
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
        c.delivery_location,
        c.sterile_code_cutter,
        c.cord_stump_treated,
        c.baby_age_days,
        c.baby_suckling_normally,
        c.confirmed_nnt,
        c.baby_treated_at_facility,
        c.baby_mother_alive,
        c.title_clinical_care,
        c.type_of_care,
        c.date_of_admission,
        c.inpatient_number,
        c.name_of_health_facility,
        c.mfl_number_of_health_facility,
        c.outcome_of_patient,
        c.status_of_patient,
        c.date_of_discharge,
        c.date_of_death,
        c.laboratory_samples_were_collected,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_other,
        c.date_of_sample_collection,
        c.result_of_laboratory_test,
        c.date_of_laboratory_results,
        c.title_laboratory_facility,
        c.name_of_laboratory_facility,
        c.mfl_number_of_laboratory_facility,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (CASE WHEN c.laboratory_samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.confirmed_nnt = 'Yes' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 WHEN c.result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_neonatal_tetanus') }} c
    LEFT JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.id IS NULL 
      AND c.parent_id IS NOT NULL 
      AND c.parent_id != '' 
      AND c.id != 'a52f1b40-89b1-22df-a632-9dcf84b7c057'
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
            FROM {{ ref('int_neonatal_tetanus') }} child 
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
    h.syndrome,
    'Neonatal Tetanus'::text AS disease,
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
    h.date_of_birth,
    h.case_is_pregnant,
    h.marital_status,
    h.phone_number,
    h.occupation,
    h.occupation_other,
    h.level_of_education,
    h.case_has_insurance_cover,
    h.country,
    h.county,
    h.subcounty,
    h.ward,
    h.town_village_camp,
    h.landmark,
    h.title_residence,
    h.type_of_residence,
    h.title_parent_guardian,
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
    h.delivery_location,
    h.sterile_code_cutter,
    h.cord_stump_treated,
    h.baby_age_days,
    h.baby_suckling_normally,
    h.confirmed_nnt,
    h.baby_treated_at_facility,
    h.baby_mother_alive,
    h.title_clinical_care,
    h.type_of_care,
    h.date_of_admission,
    h.inpatient_number,
    h.name_of_health_facility,
    h.mfl_number_of_health_facility,
    h.outcome_of_patient,
    h.status_of_patient,
    h.date_of_discharge,
    h.date_of_death,
    h.laboratory_samples_were_collected,
    h.laboratory_samples_collected,
    h.laboratory_samples_collected_other,
    h.date_of_sample_collection,
    h.result_of_laboratory_test,
    h.date_of_laboratory_results,
    h.title_laboratory_facility,
    h.name_of_laboratory_facility,
    h.mfl_number_of_laboratory_facility,
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
