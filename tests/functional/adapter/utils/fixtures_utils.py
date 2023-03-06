###############################################################################################################
# fixtures for "test_date_trunc"
###############################################################################################################
seeds__data_date_trunc_csv = """updated_at,dayy,monthh
2018-01-05 12:00:00,2018-01-05,2018-01-01
,,
"""

models__test_date_trunc_sql = """
with data as (

    select * from {{ ref('data_date_trunc') }}

)

select
    cast({{date_trunc('DD', 'updated_at') }} as date) as actual,
    dayy as expected

from data

union all

select
    cast({{ date_trunc('month', 'updated_at') }} as date) as actual,
    monthh as expected

from data
"""

models__test_date_trunc_yml = """
version: 2
models:
  - name: test_date_trunc
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

########################################## END #############################################################

#############################################################################################################
# fixtures for "test_dateadd"
#############################################################################################################

seeds__data_dateadd_csv = """from_time,interval_length,datepart,resultt
2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
,1,day,
"""

models__test_dateadd_sql = """
with data as (

    select * from {{ ref('data_dateadd') }}

)

select
    case
        when datepart = 'hour' then cast({{ dateadd('hour', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'day' then cast({{ dateadd('day', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'month' then cast({{ dateadd('month', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'year' then cast({{ dateadd('year', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        else null
    end as actual,
    resultt as expected

from data
"""

models__test_dateadd_yml = """
version: 2
models:
  - name: test_dateadd
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

############################################## END ##################################################################################

#######################################################################################################################################
# fixtures for "test_datediff"
# had to make differet scenerios for different "datepart" as teradata was not supporting different dateparts in same Case statement.
#######################################################################################################################################

seeds__data_datediff_csv = """first_date,second_date,datepart,resultt
2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
"""


models__test_datediff1_sql = """
with data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'day' then ({{ datediff('first_date', 'second_date', 'day') }})
        else null
    end as actual,
    resultt as expected

from data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31'", "'2000-01-01'", "day") }} as actual, 1 as expected
from data
"""

models__test_datediff2_sql = """
with data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'month' then ({{ datediff('first_date', 'second_date', 'month') }})
        else null
    end as actual,
    resultt as expected

from data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31'", "'2000-01-01'", "month") }} as actual, 1 as expected
from data
"""

models__test_datediff3_sql = """
with data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'year' then ({{ datediff('first_date', 'second_date', 'year') }})
        else null
    end as actual,
    resultt as expected

from data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31'", "'2000-01-01'", "year") }} as actual, 1 as expected
from data
"""

models__test_datediff_yml = """
version: 2
models:
  - name: test_datediff1
    tests:
      - assert_equal:
          actual: actual
          expected: expected
  - name: test_datediff2
    tests:
      - assert_equal:
          actual: actual
          expected: expected
  - name: test_datediff3
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

############################################## END ##################################################################################

#######################################################################################################################################
# fixtures for "test_replace"
#######################################################################################################################################

seeds__data_replace_csv = """string_text,search_chars,replace_chars,resultt
a,a,b,b
http://google.com,http://,"",google.com
"""

models__test_replace_sql = """
with data as (

    select

        string_text,search_chars,replace_chars,resultt,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars

    from {{ ref('data_replace') }}

)

select

    {{ replace('string_text', 'old_chars', 'new_chars') }} as actual,
    resultt as expected

from data
"""

models__test_replace_yml = """
version: 2
models:
  - name: test_replace
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

############################################## END ##################################################################################

################################################################################################################################
# fixtures for "test_split_part"
################################################################################################################################

seeds__data_split_part_csv = """parts,split_on,result_1,result_2,result_3
a|b|c,|,a,b,c
1|2|3,|,1,2,3
,|,,,
"""


models__test_split_part_sql = """
with data as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected

from data
"""


models__test_split_part_yml = """
version: 2
models:
  - name: test_split_part
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

############################################## END ##################################################################################

################################################################################################################################
# fixtures for "test_hash"
################################################################################################################################

seeds__data_hash_csv = """input_1,output
ab,187ef4436122d1cc2f40dc2b92f0eba0
a,0cc175b9c0f1b6a831c399e269772661
1,c4ca4238a0b923820dcc509a6f75849b
,d41d8cd98f00b204e9800998ecf8427e
"""


models__test_hash_sql = """
with data as (

    select * from {{ ref('data_hash') }}

)

select
    {{ hash('input_1') }} as actual,
    output as expected

from data
"""


models__test_hash_yml = """
version: 2
models:
  - name: test_hash
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
