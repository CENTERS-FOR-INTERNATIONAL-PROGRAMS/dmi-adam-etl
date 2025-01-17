SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    county || ' (' || date_of_assessment || ')' AS contact_tree_label,
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
    ''::text AS syndrome,
    'Marburg'::text AS disease,
    county::text AS county,
    date_of_assessment::text AS date_of_assessment,
    CASE WHEN date_of_assessment ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_assessment, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_assessment ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_assessment, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
    name_of_the_validator::text AS name_of_the_validator,
    validator_phone_number::text AS validator_phone_number,
    ipc_staff_trained_vhf_cases::text AS ipc_staff_trained_vhf_cases,
    ipc_triage_system::text AS ipc_triage_system,
    ipc_hw_trained_80percent::text AS ipc_hw_trained_80percent,
    ipc_guidelines_sops::text AS ipc_guidelines_sops,
    ipc_community_volunteers_disinfection::text AS ipc_community_volunteers_disinfection,
    ipc_teams_household_disinfection::text AS ipc_teams_household_disinfection,
    ipc_safe_dignified_burial::text AS ipc_safe_dignified_burial,
    poe_contingency_plan::text AS poe_contingency_plan,
    poe_medical_examination_facilities::text AS poe_medical_examination_facilities,
    poe_mechanism_temp_monitoring::text AS poe_mechanism_temp_monitoring,
    poe_isolation_quarantine_facilities::text AS poe_isolation_quarantine_facilities,
    poe_ambulance_service_link::text AS poe_ambulance_service_link,
    poe_integrated_surveillance_system::text AS poe_integrated_surveillance_system,
    poe_staff_manage_vhf::text AS poe_staff_manage_vhf,
    poe_functional_handwashing::text AS poe_functional_handwashing,
    poe_posters_leaflets::text AS poe_posters_leaflets,
    rrt_county_trained_multidisciplinary::text AS rrt_county_trained_multidisciplinary,
    rrt_established_in_highrisk_sc::text AS rrt_established_in_highrisk_sc,
    rrt_equipment_logistics_24hr::text AS rrt_equipment_logistics_24hr,
    rrt_sops_guidelines::text AS rrt_sops_guidelines,
    wash_prepositioned_kits_80percent::text AS wash_prepositioned_kits_80percent,
    wash_80percent_hf_incinerator::text AS wash_80percent_hf_incinerator,
    wash_secure_waste_zones::text AS wash_secure_waste_zones,
    wash_80percent_water_access::text AS wash_80percent_water_access,
    wash_medical_waste_management::text AS wash_medical_waste_management,
    overall_comment::text AS overall_comment,
    financing_contingency_emergency_fund::text AS financing_contingency_emergency_fund,
    lab_pre_positioned_stock::text AS lab_pre_positioned_stock,
    lab_reverse_transcriptase_pcr_capacity::text AS lab_reverse_transcriptase_pcr_capacity,
    lab_sops_collection_transport::text AS lab_sops_collection_transport,
    lab_sops_subcounties_80percent::text AS lab_sops_subcounties_80percent,
    lab_staff_trained_sample_transport::text AS lab_staff_trained_sample_transport,
    lab_trained_staff_safety_ipc::text AS lab_trained_staff_safety_ipc,
    lab_functional_reporting_system::text AS lab_functional_reporting_system,
    lab_capacity_for_sequencing::text AS lab_capacity_for_sequencing,
    vaccination_import_clearing_procedures::text AS vaccination_import_clearing_procedures,
    vaccination_strategy_developed::text AS vaccination_strategy_developed,
    vaccination_initiated_80percent_hotspots::text AS vaccination_initiated_80percent_hotspots,
    vaccination_partners_mass_immunisation::text AS vaccination_partners_mass_immunisation,
    srv_ph_personnel_idsr_80percent::text AS srv_ph_personnel_idsr_80percent,
    srv_public_health_facilities_reporting::text AS srv_public_health_facilities_reporting,
    srv_community_based_definitions_80percent::text AS srv_community_based_definitions_80percent,
    srv_community_volunteers_training::text AS srv_community_volunteers_training,
    srv_private_health_facilities_vhf::text AS srv_private_health_facilities_vhf,
    srv_risk_assessment_mapping::text AS srv_risk_assessment_mapping,
    srv_hotspots_list_updated::text AS srv_hotspots_list_updated,
    srv_adequate_reporting_tools::text AS srv_adequate_reporting_tools,
    srv_alert_desks::text AS srv_alert_desks,
    srv_capacity_person_place_time::text AS srv_capacity_person_place_time,
    srv_hw_trained_in_ebs::text AS srv_hw_trained_in_ebs,
    cm_guidelines_protocols::text AS cm_guidelines_protocols,
    cm_staff_trained_80percent::text AS cm_staff_trained_80percent,
    cm_referral_system_80percent::text AS cm_referral_system_80percent,
    geninfo_total_population::text AS geninfo_total_population,
    geninfo_number_of_subcounties::text AS geninfo_number_of_subcounties,
    geninfo_high_risk_subcounties::text AS geninfo_high_risk_subcounties,
    geninfo_health_facilities::text AS geninfo_health_facilities,
    geninfo_hf_in_high_risk_sc::text AS geninfo_hf_in_high_risk_sc,
    geninfo_hcw_in_high_risk_counties::text AS geninfo_hcw_in_high_risk_counties,
    geninfo_hw_trained_idsr::text AS geninfo_hw_trained_idsr,
    geninfo_hf_reporting_mvd_idsr::text AS geninfo_hf_reporting_mvd_idsr,
    geninfo_hf_with_case_defs_mvd::text AS geninfo_hf_with_case_defs_mvd,
    geninfo_hf_mvd_investigation_linelisting::text AS geninfo_hf_mvd_investigation_linelisting,
    geninfo_hf_with_cbs_structures::text AS geninfo_hf_with_cbs_structures,
    geninfo_chv_in_high_risk_communities::text AS geninfo_chv_in_high_risk_communities,
    geninfo_chv_trained_cbs_reporting::text AS geninfo_chv_trained_cbs_reporting,
    geninfo_hf_in_high_risk_sc_with_vtm_rdt::text AS geninfo_hf_in_high_risk_sc_with_vtm_rdt,
    logproc_case_mgmt_facilities_80percent::text AS logproc_case_mgmt_facilities_80percent,
    logproc_hr_isolation_units::text AS logproc_hr_isolation_units,
    logproc_essential_stocks_80percent::text AS logproc_essential_stocks_80percent,
    logproc_stock_monitoring_system_80percent::text AS logproc_stock_monitoring_system_80percent,
    logproc_suppliers_list::text AS logproc_suppliers_list,
    hrh_surge_staff_in_place::text AS hrh_surge_staff_in_place,
    hrh_clinical_staff_coverage::text AS hrh_clinical_staff_coverage,
    hrh_task_shifting_protocols::text AS hrh_task_shifting_protocols,
    coord_multisectoral_focal_points::text AS coord_multisectoral_focal_points,
    coord_multisectoral_mechanism::text AS coord_multisectoral_mechanism,
    coord_kpis_monitoring_quality_control::text AS coord_kpis_monitoring_quality_control,
    coord_resource_mobilisation_plan::text AS coord_resource_mobilisation_plan,
    coord_risk_subcounties_multi_sectoral::text AS coord_risk_subcounties_multi_sectoral,
    coord_updated_5w_of_partners::text AS coord_updated_5w_of_partners,
    coord_simulation_exercise::text AS coord_simulation_exercise,
    rcce_spokespersons_trained::text AS rcce_spokespersons_trained,
    rcce_media_partnerships::text AS rcce_media_partnerships,
    rcce_social_mobilisation_mechanisms::text AS rcce_social_mobilisation_mechanisms,
    rcce_trained_community_volunteers::text AS rcce_trained_community_volunteers,
    rcce_key_messages_local_languages::text AS rcce_key_messages_local_languages,
    rcce_platform_engage_community_leaders::text AS rcce_platform_engage_community_leaders,
    rcce_existing_chws_in_high_risk::text AS rcce_existing_chws_in_high_risk,
    rcce_communication_platforms_social::text AS rcce_communication_platforms_social,
    rcce_pre_post_outbreak_assessment::text AS rcce_pre_post_outbreak_assessment,
    rcce_national_plan_in_place::text AS rcce_national_plan_in_place
FROM {{ ref('stg_mvd_preparedness') }}