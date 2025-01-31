{{ config(
    materialized = 'table',
    indexes=[
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

WITH acute_flaccid_paralysis AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN date_first_specimen_collected IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN afp_final_case_classification = 'Confirmed' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_acute_flaccid_paralysis') }} AS int_acute_flaccid_paralysis
), community_led_total_sanitation AS (
    SELECT
        'Community Led Total Sanitation' AS syndrome,
        null::text AS disease,
        case_date,
        epi_week,
        'Unknown'::text AS type_of_case,
        null::text AS sex,
        null::text AS age_group,
        null::text AS country,
        null::text AS county,
        subcounty::text AS subcounty,
        (1)::integer AS suspected,
        (0)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        (0)::integer AS probable,
        (0)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        ''::text AS completed
    FROM {{ ref('int_community_led_total_sanitation') }}
), diarrhoeal_disease AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN rdt_done = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN rdt_results = 'Positive' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM
        {{ ref('int_diarrhoeal_disease') }}  as int_diarrhoeal_disease
), marburg_traveller AS (
    SELECT
        syndrome,
        'Marburg' AS disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        null::integer AS tested,
        null::integer AS confirmed,
        null::integer AS admitted,
        null::integer AS discharged,
        null::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_marburg_traveler') }}
), measles AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN were_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN laboratory_results = 'Positive' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_measles') }}
), meningitis AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN laboratory_test_requested IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN patient_status IN ('Admitted', 'Discharged', 'Dead') THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN patient_status = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_meningitis') }}
), mpox AS (
    SELECT
        syndrome,
        'Monkey Pox' AS disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome_of_patient = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome_of_patient = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_mpox') }}
), neonatal_tetanus AS (
    SELECT
        ''::text AS syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN laboratory_samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN confirmed_nnt = 'Yes' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_neonatal_tetanus') }}
), rabies AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN admitted = 'Yes' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_rabies') }}
), respiratory_syndrome AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN were_laboratory_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN result_of_laboratory_test = 'Positive' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN date_of_admission IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN date_of_discharge IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN date_of_death IS NOT NULL THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_respiratory_syndrome') }}
), sampling_form_for_fortified_foods AS (
    SELECT
        'Sampling Form for Fortified Foods' AS syndrome,
        disease,
        case_date,
        epi_week,
        'Unknown'::text AS type_of_case,
        null::text AS sex,
        null::text AS age_group,
        null::text AS country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN date_of_sample_collection IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        (0)::integer AS probable,
        (0)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        ''::text AS completed
    FROM {{ ref('int_sampling_form_for_fortified_foods') }}
), viral_hemorrhagic_fever AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        CASE WHEN type_of_case IS NOT NULL THEN type_of_case ELSE 'Unknown' END AS type_of_case,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN rdt_done = 'Yes' THEN 1 WHEN laboratory_sample_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN rdt_results = 'Positive' THEN 1 WHEN outcome_final_case_classification = 'Confirmed' THEN 1 WHEN type_of_case = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN admitted = 'Yes' THEN 1 WHEN outcome = 'Admitted' THEN 1 WHEN admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 WHEN discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Died' THEN 1 WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        (CASE WHEN type_of_case = 'Probable' THEN 1 ELSE 0 END)::integer AS probable,
        (CASE WHEN type_of_case = 'Contact' THEN 1 ELSE 0 END)::integer AS contact,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date,
        completed
    FROM {{ ref('int_viral_hemorrhagic_fever') }}
)

SELECT * FROM acute_flaccid_paralysis
UNION
SELECT * FROM community_led_total_sanitation
UNION
SELECT * FROM diarrhoeal_disease
UNION
SELECT * FROM measles
UNION
SELECT * FROM meningitis
UNION
SELECT * FROM mpox
UNION
SELECT * FROM neonatal_tetanus
UNION
SELECT * FROM rabies
UNION
SELECT * FROM respiratory_syndrome
UNION
SELECT * FROM sampling_form_for_fortified_foods
UNION
SELECT * FROM viral_hemorrhagic_fever
