from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import(
    min,
    max,
    col,
    round as round_col
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

