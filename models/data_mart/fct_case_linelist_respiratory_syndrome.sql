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
        'a52f1b40-89b1-22df-a632-9dcf84b7c059'::text AS id,
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
        'Respiratory Syndrome'::text AS syndrome,
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
        NULL::date AS date_of_birth,
        NULL AS case_is_pregnant,
        NULL AS phone_number,
        NULL AS marital_status,
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
        NULL AS vaccination_received,
        NULL AS vaccination_received_other,
        NULL AS name_of_vaccine,
        NULL AS date_of_vaccination,
        NULL AS number_of_doses,
        NULL AS case_travelled,
        NULL AS case_risk_behaviours,
        NULL AS case_chronic_disease_other,
        NULL AS case_risk_behaviours_other,
        NULL AS case_visited_health_facility,
        NULL AS case_underlying_medical_conditions,
        NULL AS case_has_underlying_medical_condition,
        NULL AS case_contact_with_suspected_or_confirmed_case,
        NULL AS date_of_first_contact_with_healthcare_system,
        NULL AS symptoms,
        NULL AS symptoms_other,
        NULL AS date_of_onset,
        NULL AS title_clinical_care,
        NULL AS type_of_care,
        NULL AS health_facility_name,
        NULL AS health_facility_mfl_number,
        NULL AS date_of_admission,
        NULL AS inpatient_number,
        NULL AS outpatient_number,
        NULL AS outcome_of_patient,
        NULL AS date_of_death,
        NULL AS status_of_patient,
        NULL AS date_of_discharge,
        NULL AS were_laboratory_samples_collected,
        NULL AS laboratory_samples_collected,
        NULL AS date_of_sample_collection,
        NULL AS laboratory_test_conducted,
        NULL AS laboratory_test_conducted_other,
        NULL AS date_of_laboratory_testing,
        NULL AS date_of_laboratory_testing_results,
        NULL AS result_of_laboratory_test,
        NULL AS laboratory_facility_name,
        NULL AS laboratory_facility_name_other,
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
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c059'
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
        'Respiratory Syndrome'::text AS syndrome,
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
        c.phone_number,
        c.marital_status,
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
        c.vaccination_received,
        c.vaccination_received_other,
        c.name_of_vaccine,
        c.date_of_vaccination,
        c.number_of_doses,
        c.case_travelled,
        c.case_risk_behaviours,
        c.case_chronic_disease_other,
        c.case_risk_behaviours_other,
        c.case_visited_health_facility,
        c.case_underlying_medical_conditions,
        c.case_has_underlying_medical_condition,
        c.case_contact_with_suspected_or_confirmed_case,
        c.date_of_first_contact_with_healthcare_system,
        c.symptoms,
        c.symptoms_other,
        c.date_of_onset,
        c.title_clinical_care,
        c.type_of_care,
        c.health_facility_name,
        c.health_facility_mfl_number,
        c.date_of_admission,
        c.inpatient_number,
        c.outpatient_number,
        c.outcome_of_patient,
        c.date_of_death,
        c.status_of_patient,
        c.date_of_discharge,
        c.were_laboratory_samples_collected,
        c.laboratory_samples_collected,
        c.date_of_sample_collection,
        c.laboratory_test_conducted,
        c.laboratory_test_conducted_other,
        c.date_of_laboratory_testing,
        c.date_of_laboratory_testing_results,
        c.result_of_laboratory_test,
        c.laboratory_facility_name,
        c.laboratory_facility_name_other,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (CASE WHEN c.were_laboratory_samples_collected = 'Yes' THEN 1 WHEN c.laboratory_test_conducted IS NOT NULL THEN 1 WHEN c.laboratory_test_conducted_other IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.result_of_laboratory_test = 'Positive' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.date_of_admission IS NOT NULL AND c.date_of_admission != '' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.date_of_discharge IS NOT NULL AND c.date_of_discharge != '' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.date_of_death IS NOT NULL AND c.date_of_death != '' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_respiratory_syndrome') }} c
    INNER JOIN hierarchy h ON 
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c059'
            ELSE c.parent_id
        END = h.id
    WHERE NOT (c.id = 'a52f1b40-89b1-22df-a632-9dcf84b7c059')
),

first_level_orphans AS (
    SELECT 
        c.id,
        'a52f1b40-89b1-22df-a632-9dcf84b7c059' AS parent_id,
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
        'Respiratory Syndrome'::text AS syndrome,
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
        c.phone_number,
        c.marital_status,
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
        c.vaccination_received,
        c.vaccination_received_other,
        c.name_of_vaccine,
        c.date_of_vaccination,
        c.number_of_doses,
        c.case_travelled,
        c.case_risk_behaviours,
        c.case_chronic_disease_other,
        c.case_risk_behaviours_other,
        c.case_visited_health_facility,
        c.case_underlying_medical_conditions,
        c.case_has_underlying_medical_condition,
        c.case_contact_with_suspected_or_confirmed_case,
        c.date_of_first_contact_with_healthcare_system,
        c.symptoms,
        c.symptoms_other,
        c.date_of_onset,
        c.title_clinical_care,
        c.type_of_care,
        c.health_facility_name,
        c.health_facility_mfl_number,
        c.date_of_admission,
        c.inpatient_number,
        c.outpatient_number,
        c.outcome_of_patient,
        c.date_of_death,
        c.status_of_patient,
        c.date_of_discharge,
        c.were_laboratory_samples_collected,
        c.laboratory_samples_collected,
        c.date_of_sample_collection,
        c.laboratory_test_conducted,
        c.laboratory_test_conducted_other,
        c.date_of_laboratory_testing,
        c.date_of_laboratory_testing_results,
        c.result_of_laboratory_test,
        c.laboratory_facility_name,
        c.laboratory_facility_name_other,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (CASE WHEN c.were_laboratory_samples_collected = 'Yes' THEN 1 WHEN c.laboratory_test_conducted IS NOT NULL THEN 1 WHEN c.laboratory_test_conducted_other IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.result_of_laboratory_test = 'Positive' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.date_of_admission IS NOT NULL AND c.date_of_admission != '' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.date_of_discharge IS NOT NULL AND c.date_of_discharge != '' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.date_of_death IS NOT NULL AND c.date_of_death != '' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_respiratory_syndrome') }} c
    LEFT JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.id IS NULL 
      AND c.parent_id IS NOT NULL 
      AND c.parent_id != '' 
      AND c.id != 'a52f1b40-89b1-22df-a632-9dcf84b7c059'
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
            FROM {{ ref('int_respiratory_syndrome') }} child 
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
    'Respiratory Syndrome'::text AS syndrome,
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
    h.date_of_birth,
    h.case_is_pregnant,
    h.phone_number,
    h.marital_status,
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
    h.vaccination_received,
    h.vaccination_received_other,
    h.name_of_vaccine,
    h.date_of_vaccination,
    h.number_of_doses,
    h.case_travelled,
    h.case_risk_behaviours,
    h.case_chronic_disease_other,
    h.case_risk_behaviours_other,
    h.case_visited_health_facility,
    h.case_underlying_medical_conditions,
    h.case_has_underlying_medical_condition,
    h.case_contact_with_suspected_or_confirmed_case,
    h.date_of_first_contact_with_healthcare_system,
    h.symptoms,
    h.symptoms_other,
    h.date_of_onset,
    h.title_clinical_care,
    h.type_of_care,
    h.health_facility_name,
    h.health_facility_mfl_number,
    h.date_of_admission,
    h.inpatient_number,
    h.outpatient_number,
    h.outcome_of_patient,
    h.date_of_death,
    h.status_of_patient,
    h.date_of_discharge,
    h.were_laboratory_samples_collected,
    h.laboratory_samples_collected,
    h.date_of_sample_collection,
    h.laboratory_test_conducted,
    h.laboratory_test_conducted_other,
    h.date_of_laboratory_testing,
    h.date_of_laboratory_testing_results,
    h.result_of_laboratory_test,
    h.laboratory_facility_name,
    h.laboratory_facility_name_other,
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
