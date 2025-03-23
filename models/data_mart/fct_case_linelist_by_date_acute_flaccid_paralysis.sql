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
