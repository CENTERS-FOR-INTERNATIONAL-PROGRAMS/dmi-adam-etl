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

SELECT * FROM {{ ref('fct_case_linelist_by_date_acute_flaccid_paralysis') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_community_led_total_sanitation') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_diarrhoeal_diseases') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_measles') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_meningitis') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_mpox') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_neonatal_tetanus') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_rabies') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_respiratory_syndrome') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_sampling_form_for_fortified_foods') }}
UNION
SELECT * FROM {{ ref('fct_case_linelist_by_date_viral_hemorrhagic_fever') }}
