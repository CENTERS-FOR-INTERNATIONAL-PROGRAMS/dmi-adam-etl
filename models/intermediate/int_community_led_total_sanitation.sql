SELECT
    mform_id::text AS mform_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    CASE WHEN clts_triggering_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(clts_triggering_date, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN clts_triggering_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(clts_triggering_date, 'DD/MM/YYYY'), 'YYYY"-"IW') ELSE NULL END AS epi_week,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    village::text AS village,
    sublocation_name::text AS sublocation,
    commuity_unit::text AS commuity_unit,
    location_name::text AS location,
    ward_name::text AS ward,
    subcounty::text AS subcounty,
    clts_triggering_date::text AS clts_triggering_date,
    clts_hcw_name::text AS clts_hcw_name,
    clts_hcw_contact::text AS clts_hcw_contact,
    clts_natural_leader_name::text AS clts_natural_leader_name,
    clts_natural_leader_phone_number::text AS clts_natural_leader_phone_number,
    household_head_name::text AS household_head_name,
    clts_number_of_people::text AS clts_number_of_people,
    clts_male_number::text AS clts_male_number,
    clts_female_number::text AS clts_female_number,
    clts_children_number::text AS clts_children_number,
    clts_at_trigger::text AS clts_at_trigger,
    clts_household_latrine_before_trigger::text AS clts_household_latrine_before_trigger,
    clts_household_handwashing_before_trigger::text AS clts_household_handwashing_before_trigger,
    clts_commitments::text AS clts_commitments,
    clts_commitment_latrine_construction_date::text AS clts_commitment_latrine_construction_date,
    clts_commitment_latrine_under_construction::text AS clts_commitment_latrine_under_construction,
    clts_followup_latrine_construction_date::text AS clts_commitment_latrine_completion_date,
    title_clts_follow_up::text AS title_clts_follow_up,
    clts_commitment_new_latrine_constructed::text AS clts_commitment_new_latrine_constructed,
    ctls_follow_up_handwashing_facility_constructed::text AS ctls_follow_up_handwashing_facility_constructed,
    clts_followup_latrine_construction_date::text AS clts_followup_latrine_construction_date
FROM {{ ref('stg_community_led_total_sanitation') }}
