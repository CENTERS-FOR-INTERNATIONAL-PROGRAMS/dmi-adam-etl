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
        'a52f1b40-89b1-22df-a632-9dcf84b7c060'::text AS id,
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
        'VHF'::text AS syndrome,
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
        NULL AS country,
        NULL AS county,
        NULL AS subcounty,
        NULL AS ward,
        NULL AS town_village_camp,
        NULL AS phone_number,
        NULL AS occupation,
        NULL AS occupation_other,
        NULL AS marital_status,
        NULL AS case_is_pregnant,
        NULL AS case_has_insurance_cover,
        NULL AS level_of_education,
        NULL AS type_of_residence,
        NULL AS title_residence,
        NULL AS landmark,
        NULL AS title_parent_guardian,
        NULL AS guardian_family_name,
        NULL AS guardian_given_name,
        NULL AS guardian_phone_number,
        NULL AS title_respondent,
        NULL AS respondent,
        NULL AS respondent_family_name,
        NULL AS respondent_given_name,
        NULL AS respondent_phone_number,
        NULL AS respondent_relationship,
        NULL AS respondent_relationship_other,
        NULL AS respondent_address,
        NULL AS contact_surname,
        NULL AS contact_given_name,
        NULL AS contact_sex,
        NULL AS contact_age,
        NULL AS outcome_final_case_classification,
        NULL AS outcome_final_patient_status,
        NULL AS outcome_final_patient_status_other,
        NULL AS outcome_reason_for_referral,
        NULL AS outcome_date_of_outcome,
        NULL AS record_exposure,
        NULL AS exposure_type,
        NULL AS type_of_exposure,
        NULL AS food_name,
        NULL AS food_consumption_date,
        NULL AS food_source,
        NULL AS food_participants_count,
        NULL AS food_affected_participants_count,
        NULL AS water_sources,
        NULL AS water_sources_other,
        NULL AS toilet_types,
        NULL AS toilet_types_other,
        NULL AS travel_origin_country,
        NULL AS travel_origin_city,
        NULL AS travel_departure_date,
        NULL AS travel_destination_country,
        NULL AS travel_destination_city,
        NULL AS travel_arrival_date,
        NULL AS animal_exposure,
        NULL AS animal_exposure_other,
        NULL AS animal_species,
        NULL AS animal_species_other,
        NULL AS animal_exposure_location,
        NULL AS animal_exposure_date,
        NULL AS type_of_case_contact,
        NULL AS type_of_direct_contact,
        NULL AS type_of_direct_contact_other,
        NULL AS case_was_symptomatic_during_contact,
        NULL AS date_of_contact,
        NULL AS frequency_of_contact,
        NULL AS duration_of_contact,
        NULL AS location_of_contact,
        NULL AS relationship,
        NULL AS relationship_other,
        NULL AS title_ppe,
        NULL AS case_was_wearing_ppe,
        NULL AS ppe_items_worn,
        NULL AS ppe_breach,
        NULL AS hand_hygiene_before_wearing_ppe,
        NULL AS hand_hygiene_after_wearing_ppe,
        NULL AS description_of_event,
        NULL AS location_of_event,
        NULL AS date_of_event,
        NULL AS case_was_ill,
        NULL AS description_of_illness,
        NULL AS case_seen_at_facility,
        NULL AS date_first_seen_at_facility,
        NULL AS case_admitted,
        NULL AS health_facility_name,
        NULL AS admission_date,
        NULL AS inpatient_number,
        NULL AS discharge_date,
        NULL AS patient_status,
        NULL AS laboratory_sample_collected,
        NULL AS laboratory_samples_collected_other,
        NULL AS date_of_sample_collection,
        NULL AS laboratory_samples_collected,
        NULL AS laboratory_samples_collected_others,
        NULL AS sample_date,
        NULL AS rdt_done,
        NULL AS rdt_results,
        NULL AS samples_sent_to_laboratory,
        NULL AS laboratory_name,
        NULL AS sample_sent_to_lab_date,
        NULL AS symptoms,
        NULL AS symptoms_other,
        NULL AS symptoms_unexplained_bleeding,
        NULL AS symptoms_start_date,
        NULL AS date_of_onset,
        NULL AS title_symptoms_start_location,
        NULL AS symptoms_start_county,
        NULL AS symptoms_start_subcounty,
        NULL AS symptoms_start_city_town_village_camp,
        NULL AS outcome,
        NULL AS title_clinical_care,
        NULL AS type_of_care,
        NULL AS name_of_health_facility,
        NULL AS date_of_admission,
        NULL AS ward_of_admission,
        NULL AS outpatient_number,
        NULL AS outcome_of_case,
        NULL AS status_of_patient,
        NULL AS date_of_discharge,
        NULL AS date_of_death,
        NULL AS place_of_death,
        NULL AS place_of_burial,
        NULL AS laboratory_tests,
        NULL AS title_laboratory_facility,
        NULL AS type_of_laboratory_facility,
        NULL AS name_of_laboratory_facility,
        NULL AS name_of_laboratory_facility_other,
        NULL AS date_of_laboratory_testing_results,
        NULL AS vaccination_ebola_vaccine_received,
        NULL AS vaccination_ebola_name_of_vaccine,
        NULL AS vaccination_ebola_number_of_doses,
        NULL AS vaccination_ebola_date_of_vaccination,
        NULL AS contact_follow_up_day_of_follow_up,
        NULL AS contact_follow_up_date_of_follow_up,
        NULL AS contact_follow_up_contact_is_symptomatic,
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
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c060'
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
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.town_village_camp,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.marital_status,
        c.case_is_pregnant,
        c.case_has_insurance_cover,
        c.level_of_education,
        c.type_of_residence,
        c.title_residence,
        c.landmark,
        c.title_parent_guardian,
        c.guardian_family_name,
        c.guardian_given_name,
        c.guardian_phone_number,
        c.title_respondent,
        c.respondent,
        c.respondent_family_name,
        c.respondent_given_name,
        c.respondent_phone_number,
        c.respondent_relationship,
        c.respondent_relationship_other,
        c.respondent_address,
        c.contact_surname,
        c.contact_given_name,
        c.contact_sex,
        c.contact_age,
        c.outcome_final_case_classification,
        c.outcome_final_patient_status,
        c.outcome_final_patient_status_other,
        c.outcome_reason_for_referral,
        c.outcome_date_of_outcome,
        c.record_exposure,
        c.exposure_type,
        c.type_of_exposure,
        c.food_name,
        c.food_consumption_date,
        c.food_source,
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
        c.animal_exposure,
        c.animal_exposure_other,
        c.animal_species,
        c.animal_species_other,
        c.animal_exposure_location,
        c.animal_exposure_date,
        c.type_of_case_contact,
        c.type_of_direct_contact,
        c.type_of_direct_contact_other,
        c.case_was_symptomatic_during_contact,
        c.date_of_contact,
        c.frequency_of_contact,
        c.duration_of_contact,
        c.location_of_contact,
        c.relationship,
        c.relationship_other,
        c.title_ppe,
        c.case_was_wearing_ppe,
        c.ppe_items_worn,
        c.ppe_breach,
        c.hand_hygiene_before_wearing_ppe,
        c.hand_hygiene_after_wearing_ppe,
        c.description_of_event,
        c.location_of_event,
        c.date_of_event,
        c.case_was_ill,
        c.description_of_illness,
        c.case_seen_at_facility,
        c.date_first_seen_at_facility,
        c.admitted AS case_admitted,
        c.health_facility_name,
        c.admission_date,
        c.inpatient_number,
        c.discharge_date,
        c.patient_status,
        c.laboratory_sample_collected,
        c.laboratory_samples_collected_other,
        c.date_of_sample_collection,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_others,
        c.sample_date,
        c.rdt_done,
        c.rdt_results,
        c.samples_sent_to_laboratory,
        c.laboratory_name,
        c.sample_sent_to_lab_date,
        c.symptoms,
        c.symptoms_other,
        c.symptoms_unexplained_bleeding,
        c.symptoms_start_date,
        c.date_of_onset,
        c.title_symptoms_start_location,
        c.symptoms_start_county,
        c.symptoms_start_subcounty,
        c.symptoms_start_city_town_village_camp,
        c.outcome,
        c.title_clinical_care,
        c.type_of_care,
        c.name_of_health_facility,
        c.date_of_admission,
        c.ward_of_admission,
        c.outpatient_number,
        c.outcome_of_case,
        c.status_of_patient,
        c.date_of_discharge,
        c.date_of_death,
        c.place_of_death,
        c.place_of_burial,
        c.laboratory_tests,
        c.title_laboratory_facility,
        c.type_of_laboratory_facility,
        c.name_of_laboratory_facility,
        c.name_of_laboratory_facility_other,
        c.date_of_laboratory_testing_results,
        c.vaccination_ebola_vaccine_received,
        c.vaccination_ebola_name_of_vaccine,
        c.vaccination_ebola_number_of_doses,
        c.vaccination_ebola_date_of_vaccination,
        c.contact_follow_up_day_of_follow_up,
        c.contact_follow_up_date_of_follow_up,
        c.contact_follow_up_contact_is_symptomatic,
        (1)::integer AS total,
        (CASE WHEN c.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN c.rdt_done = 'Yes' THEN 1 WHEN c.laboratory_sample_collected = 'Yes' THEN 1 WHEN c.laboratory_tests IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.rdt_results = 'Positive' THEN 1 WHEN c.outcome_final_case_classification = 'Confirmed' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.admitted = 'Yes' THEN 1 WHEN c.outcome = 'Admitted' THEN 1 WHEN c.admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.patient_status = 'Discharged' THEN 1 WHEN c.discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.patient_status = 'Died' THEN 1 WHEN c.outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_viral_hemorrhagic_fever') }} c
    INNER JOIN hierarchy h ON 
        CASE 
            WHEN c.parent_id IS NULL OR c.parent_id = '' THEN 'a52f1b40-89b1-22df-a632-9dcf84b7c060'
            ELSE c.parent_id
        END = h.id
    WHERE NOT (c.id = 'a52f1b40-89b1-22df-a632-9dcf84b7c060')
),

first_level_orphans AS (
    SELECT 
        c.id,
        'a52f1b40-89b1-22df-a632-9dcf84b7c060' AS parent_id,
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
        c.country,
        c.county,
        c.subcounty,
        c.ward,
        c.town_village_camp,
        c.phone_number,
        c.occupation,
        c.occupation_other,
        c.marital_status,
        c.case_is_pregnant,
        c.case_has_insurance_cover,
        c.level_of_education,
        c.type_of_residence,
        c.title_residence,
        c.landmark,
        c.title_parent_guardian,
        c.guardian_family_name,
        c.guardian_given_name,
        c.guardian_phone_number,
        c.title_respondent,
        c.respondent,
        c.respondent_family_name,
        c.respondent_given_name,
        c.respondent_phone_number,
        c.respondent_relationship,
        c.respondent_relationship_other,
        c.respondent_address,
        c.contact_surname,
        c.contact_given_name,
        c.contact_sex,
        c.contact_age,
        c.outcome_final_case_classification,
        c.outcome_final_patient_status,
        c.outcome_final_patient_status_other,
        c.outcome_reason_for_referral,
        c.outcome_date_of_outcome,
        c.record_exposure,
        c.exposure_type,
        c.type_of_exposure,
        c.food_name,
        c.food_consumption_date,
        c.food_source,
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
        c.animal_exposure,
        c.animal_exposure_other,
        c.animal_species,
        c.animal_species_other,
        c.animal_exposure_location,
        c.animal_exposure_date,
        c.type_of_case_contact,
        c.type_of_direct_contact,
        c.type_of_direct_contact_other,
        c.case_was_symptomatic_during_contact,
        c.date_of_contact,
        c.frequency_of_contact,
        c.duration_of_contact,
        c.location_of_contact,
        c.relationship,
        c.relationship_other,
        c.title_ppe,
        c.case_was_wearing_ppe,
        c.ppe_items_worn,
        c.ppe_breach,
        c.hand_hygiene_before_wearing_ppe,
        c.hand_hygiene_after_wearing_ppe,
        c.description_of_event,
        c.location_of_event,
        c.date_of_event,
        c.case_was_ill,
        c.description_of_illness,
        c.case_seen_at_facility,
        c.date_first_seen_at_facility,
        c.admitted AS case_admitted,
        c.health_facility_name,
        c.admission_date,
        c.inpatient_number,
        c.discharge_date,
        c.patient_status,
        c.laboratory_sample_collected,
        c.laboratory_samples_collected_other,
        c.date_of_sample_collection,
        c.laboratory_samples_collected,
        c.laboratory_samples_collected_others,
        c.sample_date,
        c.rdt_done,
        c.rdt_results,
        c.samples_sent_to_laboratory,
        c.laboratory_name,
        c.sample_sent_to_lab_date,
        c.symptoms,
        c.symptoms_other,
        c.symptoms_unexplained_bleeding,
        c.symptoms_start_date,
        c.date_of_onset,
        c.title_symptoms_start_location,
        c.symptoms_start_county,
        c.symptoms_start_subcounty,
        c.symptoms_start_city_town_village_camp,
        c.outcome,
        c.title_clinical_care,
        c.type_of_care,
        c.name_of_health_facility,
        c.date_of_admission,
        c.ward_of_admission,
        c.outpatient_number,
        c.outcome_of_case,
        c.status_of_patient,
        c.date_of_discharge,
        c.date_of_death,
        c.place_of_death,
        c.place_of_burial,
        c.laboratory_tests,
        c.title_laboratory_facility,
        c.type_of_laboratory_facility,
        c.name_of_laboratory_facility,
        c.name_of_laboratory_facility_other,
        c.date_of_laboratory_testing_results,
        c.vaccination_ebola_vaccine_received,
        c.vaccination_ebola_name_of_vaccine,
        c.vaccination_ebola_number_of_doses,
        c.vaccination_ebola_date_of_vaccination,
        c.contact_follow_up_day_of_follow_up,
        c.contact_follow_up_date_of_follow_up,
        c.contact_follow_up_contact_is_symptomatic,
        (1)::integer AS total,
        (CASE WHEN c.type_of_case <> 'Contact' THEN 1 ELSE 0 END)::integer AS suspected,
        (CASE WHEN c.rdt_done = 'Yes' THEN 1 WHEN c.laboratory_sample_collected = 'Yes' THEN 1 WHEN c.laboratory_tests IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN c.rdt_results = 'Positive' THEN 1 WHEN c.outcome_final_case_classification = 'Confirmed' THEN 1 WHEN c.type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN c.admitted = 'Yes' THEN 1 WHEN c.outcome = 'Admitted' THEN 1 WHEN c.admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN c.patient_status = 'Discharged' THEN 1 WHEN c.discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN c.patient_status = 'Died' THEN 1 WHEN c.outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN c.type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN c.type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        current_date AS load_date
    FROM {{ ref('int_viral_hemorrhagic_fever') }} c
    LEFT JOIN hierarchy h ON c.parent_id = h.id
    WHERE h.id IS NULL 
      AND c.parent_id IS NOT NULL 
      AND c.parent_id != '' 
      AND c.id != 'a52f1b40-89b1-22df-a632-9dcf84b7c060'
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
            FROM {{ ref('int_viral_hemorrhagic_fever') }} child 
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
    'VHF'::text AS syndrome,
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
    h.country,
    h.county,
    h.subcounty,
    h.ward,
    h.town_village_camp,
    h.phone_number,
    h.occupation,
    h.occupation_other,
    h.marital_status,
    h.case_is_pregnant,
    h.case_has_insurance_cover,
    h.level_of_education,
    h.type_of_residence,
    h.title_residence,
    h.landmark,
    h.title_parent_guardian,
    h.guardian_family_name,
    h.guardian_given_name,
    h.guardian_phone_number,
    h.title_respondent,
    h.respondent,
    h.respondent_family_name,
    h.respondent_given_name,
    h.respondent_phone_number,
    h.respondent_relationship,
    h.respondent_relationship_other,
    h.respondent_address,
    h.contact_surname,
    h.contact_given_name,
    h.contact_sex,
    h.contact_age,
    h.outcome_final_case_classification,
    h.outcome_final_patient_status,
    h.outcome_final_patient_status_other,
    h.outcome_reason_for_referral,
    h.outcome_date_of_outcome,
    h.record_exposure,
    h.exposure_type,
    h.type_of_exposure,
    h.food_name,
    h.food_consumption_date,
    h.food_source,
    h.food_participants_count,
    h.food_affected_participants_count,
    h.water_sources,
    h.water_sources_other,
    h.toilet_types,
    h.toilet_types_other,
    h.travel_origin_country,
    h.travel_origin_city,
    h.travel_departure_date,
    h.travel_destination_country,
    h.travel_destination_city,
    h.travel_arrival_date,
    h.animal_exposure,
    h.animal_exposure_other,
    h.animal_species,
    h.animal_species_other,
    h.animal_exposure_location,
    h.animal_exposure_date,
    h.type_of_case_contact,
    h.type_of_direct_contact,
    h.type_of_direct_contact_other,
    h.case_was_symptomatic_during_contact,
    h.date_of_contact,
    h.frequency_of_contact,
    h.duration_of_contact,
    h.location_of_contact,
    h.relationship,
    h.relationship_other,
    h.title_ppe,
    h.case_was_wearing_ppe,
    h.ppe_items_worn,
    h.ppe_breach,
    h.hand_hygiene_before_wearing_ppe,
    h.hand_hygiene_after_wearing_ppe,
    h.description_of_event,
    h.location_of_event,
    h.date_of_event,
    h.case_was_ill,
    h.description_of_illness,
    h.case_seen_at_facility,
    h.date_first_seen_at_facility,
    h.case_admitted,
    h.health_facility_name,
    h.admission_date,
    h.inpatient_number,
    h.discharge_date,
    h.patient_status,
    h.laboratory_sample_collected,
    h.laboratory_samples_collected_other,
    h.date_of_sample_collection,
    h.laboratory_samples_collected,
    h.laboratory_samples_collected_others,
    h.sample_date,
    h.rdt_done,
    h.rdt_results,
    h.samples_sent_to_laboratory,
    h.laboratory_name,
    h.sample_sent_to_lab_date,
    h.symptoms,
    h.symptoms_other,
    h.symptoms_unexplained_bleeding,
    h.symptoms_start_date,
    h.date_of_onset,
    h.title_symptoms_start_location,
    h.symptoms_start_county,
    h.symptoms_start_subcounty,
    h.symptoms_start_city_town_village_camp,
    h.outcome,
    h.title_clinical_care,
    h.type_of_care,
    h.name_of_health_facility,
    h.date_of_admission,
    h.ward_of_admission,
    h.outpatient_number,
    h.outcome_of_case,
    h.status_of_patient,
    h.date_of_discharge,
    h.date_of_death,
    h.place_of_death,
    h.place_of_burial,
    h.laboratory_tests,
    h.title_laboratory_facility,
    h.type_of_laboratory_facility,
    h.name_of_laboratory_facility,
    h.name_of_laboratory_facility_other,
    h.date_of_laboratory_testing_results,
    h.vaccination_ebola_vaccine_received,
    h.vaccination_ebola_name_of_vaccine,
    h.vaccination_ebola_number_of_doses,
    h.vaccination_ebola_date_of_vaccination,
    h.contact_follow_up_day_of_follow_up,
    h.contact_follow_up_date_of_follow_up,
    h.contact_follow_up_contact_is_symptomatic,
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
