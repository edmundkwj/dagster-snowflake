from dagster import MonthlyPartitionsDefinition

start_date = '2014-03-01'
end_date = '2016-03-01'

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)