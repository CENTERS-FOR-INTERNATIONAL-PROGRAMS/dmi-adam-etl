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
      {'columns': ['completed']}
    ],
    partition_by={
      'field': 'case_date',
      'data_type': 'date',
      'granularity': 'month'
    }
)}}

{% set table_refs = [
    ref('fct_case_linelist_acute_flaccid_paralysis'),
    ref('fct_case_linelist_community_led_total_sanitation'),
    ref('fct_case_linelist_diarrhoeal_disease'),
    ref('fct_case_linelist_marburg_traveler'),
    ref('fct_case_linelist_measles'),
    ref('fct_case_linelist_meningitis'),
    ref('fct_case_linelist_mpox_preparedness'),
    ref('fct_case_linelist_mpox_traveler'),
    ref('fct_case_linelist_mpox'),
    ref('fct_case_linelist_mvd_preparedness'),
    ref('fct_case_linelist_neonatal_tetanus'),
    ref('fct_case_linelist_rabies'),
    ref('fct_case_linelist_rapid_wash_assessment_tool'),
    ref('fct_case_linelist_respiratory_syndrome'),
    ref('fct_case_linelist_sampling_form_for_fortified_foods'),
    ref('fct_case_linelist_viral_hemorrhagic_fever')
] %}

{%- set base_columns = [
    'syndrome',
    'disease',
    'case_date',
    'epi_week',
    'type_of_case',
    'sex',
    'age_group',
    'country',
    'county',
    'subcounty',
    'suspected',
    'tested',
    'confirmed',
    'admitted',
    'discharged',
    'died',
    'probable',
    'contact',
    'location_accuracy',
    'location_latitude',
    'location_longitude',
    'load_date',
    'completed'
] -%}

{% for table in table_refs %}
    SELECT
        {{ base_columns | join(',\n        ') }},
        '{{ table }}' AS source_table
    FROM {{ table }}
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}