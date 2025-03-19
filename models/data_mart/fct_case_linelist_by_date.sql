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
        {'columns': ['total']},
        {'columns': ['suspected']},
        {'columns': ['tested']},
        {'columns': ['confirmed']},
        {'columns': ['admitted']},
        {'columns': ['discharged']},
        {'columns': ['died']},
    ]
)}}

WITH case_linelist_by_date_acute_flaccid_paralysis AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'AFP'::text AS disease,
        linelist.type_of_case,
        linelist.country,
        linelist.county,
        linelist.subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        linelist.total,
        linelist.suspected,
        linelist.tested,
        linelist.confirmed,
        linelist.admitted,
        linelist.discharged,
        linelist.died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_acute_flaccid_paralysis') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_community_led_total_sanitation AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        'Community Led Total Sanitation'::text AS syndrome,
        'Community Led Total Sanitation'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_community_led_total_sanitation') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_diarrhoeal_diseases AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        'Diarrhoeal Disease'::text AS syndrome,
        disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_diarrhoeal_disease') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_measles AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'Measles'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_measles') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_meningitis AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'Meningitis'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_meningitis') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_monkey_pox AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'Monkey Pox'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_mpox') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_neonatal_tetanus AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'Neonatal Tetanus'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_neonatal_tetanus') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_rabies AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        'Rabies'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_rabies') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_respiratory_syndrome AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        'Respiratory Syndrome'::text AS syndrome,
        disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_respiratory_syndrome') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_sampling_form_for_fortified_foods AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        'Sampling Form for Fortified Foods'::text AS syndrome,
        'Sampling Form for Fortified Foods'::text AS disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_sampling_form_for_fortified_foods') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
), case_linelist_by_date_viral_hemorrhagic_fever AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        'VHF'::text AS syndrome,
        disease,
        type_of_case,
        country,
        county,
        subcounty,
        CASE WHEN linelist.case_date IS NOT NULL THEN 1 ELSE 0 END AS cases,
        total,
        suspected,
        tested,
        confirmed,
        admitted,
        discharged,
        died,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist_viral_hemorrhagic_fever') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.total = 1 AND linelist.confirmed = 1
)

SELECT * FROM case_linelist_by_date_acute_flaccid_paralysis
UNION
SELECT * FROM case_linelist_by_date_community_led_total_sanitation
UNION
SELECT * FROM case_linelist_by_date_diarrhoeal_diseases
UNION
SELECT * FROM case_linelist_by_date_measles
UNION
SELECT * FROM case_linelist_by_date_meningitis
UNION
SELECT * FROM case_linelist_by_date_monkey_pox
UNION
SELECT * FROM case_linelist_by_date_neonatal_tetanus
UNION
SELECT * FROM case_linelist_by_date_rabies
UNION
SELECT * FROM case_linelist_by_date_respiratory_syndrome
UNION
SELECT * FROM case_linelist_by_date_sampling_form_for_fortified_foods
UNION
SELECT * FROM case_linelist_by_date_viral_hemorrhagic_fever
