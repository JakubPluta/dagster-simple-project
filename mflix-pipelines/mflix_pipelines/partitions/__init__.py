from dagster import MonthlyPartitionsDefinition

start_date, end_date = "2011-01-01", "2016-01-01"

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date,
)
