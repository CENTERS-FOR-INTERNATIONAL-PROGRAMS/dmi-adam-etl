{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['syndrome']},
      {'columns': ['disease']},
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
    ]
)}}

SELECT
    id,
    parent_id,
    contact_tree_label,
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
    'AFP'::text AS disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    location_of_investigation,
    location_of_investigation_other,
    CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    date_of_birth,
    country,
    county,
    subcounty,
    ward,
    case_is_pregnant,
    marital_status,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    case_has_insurance_cover,
    type_of_residence,
    town_village_camp,
    landmark,
    title_parent_guradian,
    level_of_education,
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
    date_of_onset,
    fever_at_onset_of_paralysis,
    progressive_paralysis,
    paralysis_acute_and_flaccid,
    paralysis_asymmetrical,
    paralysis_site,
    paralysis_site_other,
    paralyzed_limb_sensitive_to_pain,
    injection_before_paralysis,
    injection_site,
    provisional_diagnosis,
    outcome,
    total_vaccine_doses,
    first_dose_date,
    second_dose_date,
    third_dose_date,
    fourth_dose_date,
    last_dose_date,
    total_opv_sia_doses,
    stool_condition,
    date_final_culture_result_available,
    date_specimen_received_in_ic_lab,
    lab_id,
    final_cell_culture_result,
    date_results_sent_to_national_epi,
    date_isolate_sent_by_national_laboratory_to_regional_laboratory,
    date_itd_results_sent_by_regional_laboratory,
    date_itd_results_received_by_county,
    date_isolate_sent_for_sequencing,
    date_final_results_sent_to_epi,
    title_final_results,
    w1_results,
    w2_results,
    w3_results,
    discordant_sabin,
    sl1_results,
    sl2_results,
    sl3_results,
    vdpv1_results,
    vdpv2_results,
    vdpv3_results,
    rnev_results,
    rnpent_results,
    date_first_specimen_collected,
    date_second_specimen_collected,
    date_specimen_sent_to_national,
    date_specimen_received_at_national,
    date_specimen_sent_to_lab,
    immunocompromised_status_suspected,
    afp_final_case_classification,
    afp_vdpv_serotype,
    place,
    duration_months,
    duration_days,
    vaccine_administered,
    date_of_vaccination,
    total_vaccine_sia_doses,
    vaccine_afp,
    vaccine_afp_other,
    number_of_doses,
    date_of_last_dose,
    vaccination_campaign,
    source_of_vaccination_information,
    source_of_vaccination_information_other,
    (1)::integer AS suspected,
    (CASE WHEN date_first_specimen_collected IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN afp_final_case_classification = 'Confirmed' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN outcome = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN outcome = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
    (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_acute_flaccid_paralysis') }}
