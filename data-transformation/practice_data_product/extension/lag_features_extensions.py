from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import(
    min,
    max,
    col,
    round as round_col,
    date_sub,
    expr
)


def calculate_rolling_average(date_column: str, target_column: str, window_size_weeks: int, category_column: str) -> DataFrame:
    num_seconds_day = 24*60*60
    window_size_seconds = 7 * window_size_weeks * num_seconds_day
    rolling_window = (
        Window.orderBy(col(date_column).cast("timestamp").cast("long"))
        .partitionBy(category_column)
        .rangeBetween(-1 * window_size_seconds, 0)
    )

    def transform_df(df: DataFrame) -> DataFrame:
        return ( df.withColumn(
                        f"min_w{window_size_weeks}",
                        round_col(min(target_column).over(rolling_window))
                    ).withColumn(
                        f"max_w{window_size_weeks}",
                        round_col(max(target_column).over(rolling_window))
        )
        )
    return transform_df

def generate_past_dates(date_column: str, week_lag: int) -> DataFrame:
    """
    generates new column in the given DataFrame that is week_lag weeks before dates in the date_column
    """
    day_lag = week_lag * 7
    new_column_name = f"date_{week_lag}weeks_ago"
    
    return lambda df: df.withColumn(
                            "breakdatetimeiso", col("breakdatetimeiso").cast("timestamp")
                        ).withColumn(
                            new_column_name, col("breakdatetimeiso") - expr(f"INTERVAL {day_lag} DAYS")
                        )
    


def calculate_lag_features(date_column: str, target_column: str, window_size_weeks: int, category_column: str)  -> DataFrame:
    """
    for a given DataFrame generate rolling average for dates week_lag before dates in the date_column 
    with window equaling window_size_weeks

    horizon  = week_lag
    window = window_size_weeks
    filter = category_column
    """
    pass

