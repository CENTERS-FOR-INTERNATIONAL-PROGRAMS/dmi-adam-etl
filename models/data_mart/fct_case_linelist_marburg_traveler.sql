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
        'a52f1b40-89b1-22df-a632-9dcf84b7c054'::text AS id,
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
        'Marburg'::text AS disease,
        null::date AS case_date,
        NULL AS epi_week,
        NULL AS epid,
        NULL AS date_of_investigation,
        NULL AS type_of_case,
        NULL AS name,
        NULL AS sex,
        null::double precision AS age_years,
        NULL AS age_group,
        NULL AS nationality,
        NULL AS id_number,
        NULL AS occupation,
        NULL AS occupation_other,
        NULL AS email_address,
        NULL AS temperature,
        NULL AS classification,
        NULL AS traveller_action,
        NULL AS traveller_action_other,
        NULL AS traveller_date_of_checking,
        NULL AS three_weeks_exposure_title,
        NULL AS participated_in_patient_care,
        NULL AS participated_in_burial,
        NULL AS one_week_exposure_title,
        NULL AS symptom_fever,
        NULL AS symptom_diarrhoea,
        NULL AS generic_headache,
        NULL AS symptom_joint_muscle_pain,
        NULL AS symptom_bone_pain,
        NULL AS symptom_unexplained_bruising,
        NULL AS symptom_unexplained_body_weakness,
        NULL AS symptom_internal_external_bleeding,
        NULL AS symptom_sore_painfull_throat,
        NULL AS symptom_cough_vomiting,
        NULL AS symptom_common_cold,
        NULL AS symptom_weight_loss,
        NULL AS date_of_arrival,
        NULL AS point_of_entry,
        NULL AS point_of_entry_other,
        NULL AS country_of_departure,
        NULL AS town_of_departure,
        NULL AS mode_of_transport,
        NULL AS registration_number_of_conveyance,
        NULL AS number_of_seat,
        NULL AS purpose_of_travel,
        NULL AS expected_duration_of_stay,
        NULL AS travelled_to_country_with_confirmed_cases,
        NULL AS travel_country,
        NULL AS travel_city_town_village,
        NULL AS travel_date_of_visit_start,
        NULL AS travel_date_of_visit_end,
        NULL AS local_county,
        NULL AS local_city_town_village,
        NULL AS local_sublocation_estate,
        NULL AS local_house_hotel,
        NULL AS local_postal_address,
        NULL AS local_phone_number_of_contact,
        NULL AS local_phone_number_used_while_in_kenya,
        NULL AS country,
        NULL AS county,
        NULL AS subcounty,
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
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c054'
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
        c.type_of_case,
        c.name,
        c.sex,
        c.age_years,
        c.age_group,
        c.nationality,
        c.id_number,
        c.occupation,
        c.occupation_other,
        c.email_address,
        c.temperature,
        c.classification,
        c.traveller_action,
        c.traveller_action_other,
        c.traveller_date_of_checking,
        c.three_weeks_exposure_title,
        c.participated_in_patient_care,
        c.participated_in_burial,
        c.one_week_exposure_title,
        c.symptom_fever,
        c.symptom_diarrhoea,
        c.generic_headache,
        c.symptom_joint_muscle_pain,
        c.symptom_bone_pain,
        c.symptom_unexplained_bruising,
        c.symptom_unexplained_body_weakness,
        c.symptom_internal_external_bleeding,
        c.symptom_sore_painfull_throat,
        c.symptom_cough_vomiting,
        c.symptom_common_cold,
        c.symptom_weight_loss,
        c.date_of_arrival,
        c.point_of_entry,
        c.point_of_entry_other,
        c.country_of_departure,
        c.town_of_departure,
        c.mode_of_transport,
        c.registration_number_of_conveyance,
        c.number_of_seat,
        c.purpose_of_travel,
        c.expected_duration_of_stay,
        c.travelled_to_country_with_confirmed_cases,
        c.travel_country,
        c.travel_city_town_village,
        c.travel_date_of_visit_start,
        c.travel_date_of_visit_end,
        c.local_county,
        c.local_city_town_village,
        c.local_sublocation_estate,
        c.local_house_hotel,
        c.local_postal_address,
        c.local_phone_number_of_contact,
        c.local_phone_number_used_while_in_kenya,
        c.country,
        c.county,
        c.subcounty,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (0)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_marburg_traveler') }} c
    INNER JOIN hierarchy h ON 
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c054'
            ELSE c.parent_id
        END = h.id
    WHERE NOT (c.id = 'a52f1b40-89b1-22df-a632-9dcf84b7c054')
),

first_level_orphans AS (
    SELECT 
        c.id,
        'a52f1b40-89b1-22df-a632-9dcf84b7c054' AS parent_id,
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
        c.type_of_case,
        c.name,
        c.sex,
        c.age_years,
        c.age_group,
        c.nationality,
        c.id_number,
        c.occupation,
        c.occupation_other,
        c.email_address,
        c.temperature,
        c.classification,
        c.traveller_action,
        c.traveller_action_other,
        c.traveller_date_of_checking,
        c.three_weeks_exposure_title,
        c.participated_in_patient_care,
        c.participated_in_burial,
        c.one_week_exposure_title,
        c.symptom_fever,
        c.symptom_diarrhoea,
        c.generic_headache,
        c.symptom_joint_muscle_pain,
        c.symptom_bone_pain,
        c.symptom_unexplained_bruising,
        c.symptom_unexplained_body_weakness,
        c.symptom_internal_external_bleeding,
        c.symptom_sore_painfull_throat,
        c.symptom_cough_vomiting,
        c.symptom_common_cold,
        c.symptom_weight_loss,
        c.date_of_arrival,
        c.point_of_entry,
        c.point_of_entry_other,
        c.country_of_departure,
        c.town_of_departure,
        c.mode_of_transport,
        c.registration_number_of_conveyance,
        c.number_of_seat,
        c.purpose_of_travel,
        c.expected_duration_of_stay,
        c.travelled_to_country_with_confirmed_cases,
        c.travel_country,
        c.travel_city_town_village,
        c.travel_date_of_visit_start,
        c.travel_date_of_visit_end,
        c.local_county,
        c.local_city_town_village,
        c.local_sublocation_estate,
        c.local_house_hotel,
        c.local_postal_address,
        c.local_phone_number_of_contact,
        c.local_phone_number_used_while_in_kenya,
        c.country,
        c.county,
        c.subcounty,
        (1)::integer AS total,
        (1)::integer AS suspected,
        (0)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_marburg_traveler') }} c
    LEFT JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.id IS NULL 
      AND c.parent_id IS NOT NULL 
      AND c.parent_id != '' 
      AND c.id != 'a52f1b40-89b1-22df-a632-9dcf84b7c054'
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
            FROM {{ ref('int_marburg_traveler') }} child 
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
    'Marburg'::text AS disease,
    h.case_date,
    h.epi_week,
    h.epid,
    h.date_of_investigation,
    CASE WHEN h.type_of_case IS NOT NULL THEN h.type_of_case ELSE 'Unknown' END AS type_of_case,
    h.name,
    h.sex,
    h.age_years,
    h.age_group,
    h.nationality,
    h.id_number,
    h.occupation,
    h.occupation_other,
    h.email_address,
    h.temperature,
    h.classification,
    h.traveller_action,
    h.traveller_action_other,
    h.traveller_date_of_checking,
    h.three_weeks_exposure_title,
    h.participated_in_patient_care,
    h.participated_in_burial,
    h.one_week_exposure_title,
    h.symptom_fever,
    h.symptom_diarrhoea,
    h.generic_headache,
    h.symptom_joint_muscle_pain,
    h.symptom_bone_pain,
    h.symptom_unexplained_bruising,
    h.symptom_unexplained_body_weakness,
    h.symptom_internal_external_bleeding,
    h.symptom_sore_painfull_throat,
    h.symptom_cough_vomiting,
    h.symptom_common_cold,
    h.symptom_weight_loss,
    h.date_of_arrival,
    h.point_of_entry,
    h.point_of_entry_other,
    h.country_of_departure,
    h.town_of_departure,
    h.mode_of_transport,
    h.registration_number_of_conveyance,
    h.number_of_seat,
    h.purpose_of_travel,
    h.expected_duration_of_stay,
    h.travelled_to_country_with_confirmed_cases,
    h.travel_country,
    h.travel_city_town_village,
    h.travel_date_of_visit_start,
    h.travel_date_of_visit_end,
    h.local_county,
    h.local_city_town_village,
    h.local_sublocation_estate,
    h.local_house_hotel,
    h.local_postal_address,
    h.local_phone_number_of_contact,
    h.local_phone_number_used_while_in_kenya,
    h.country,
    h.county,
    h.subcounty,
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
