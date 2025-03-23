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
