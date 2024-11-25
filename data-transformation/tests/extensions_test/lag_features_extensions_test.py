from datetime import datetime
import pytest
from practice_data_product.extension.lag_features_extensions import calculate_rolling_average
from pyspark.sql.types import StringType, LongType, DoubleType, StructField, StructType, TimestampType



@pytest.mark.usefixtures("spark_session")
def test_calculate_rolling_average(spark_session):

    #   given
    data = [
        (datetime.strptime('2022-11-01 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 1,),
        (datetime.strptime('2022-12-05 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 90),
        (datetime.strptime('2022-12-06 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 91),
        (datetime.strptime('2022-12-07 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 92),
    ]
    schema = StructType([StructField("breakdatetimeiso", TimestampType(), True),  
              StructField("showname", StringType(), True),
              StructField("consolidatedimpact", LongType(), True)     
    ])

    df = spark_session.createDataFrame(data, schema)

    # when
    actual_df = df.transform(calculate_rolling_average("breakdatetimeiso", "consolidatedimpact", 1, "showname"))
    # actual_df.show()

    # then 
    expected_data = [
        (datetime.strptime('2022-11-01 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 1, 1, 1),
        (datetime.strptime('2022-12-05 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 90, 90, 90),
        (datetime.strptime('2022-12-06 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 91, 90, 91),
        (datetime.strptime('2022-12-07 12:08:00', "%Y-%m-%d %H:%M:%S"), 'A', 92, 90, 92),
    ]

    expected_schema = StructType([
            StructField("breakdatetimeiso", TimestampType(), True),  
            StructField("showname", StringType(), True),
            StructField("consolidatedimpact", LongType(), True),
            StructField("min_w1", LongType(), True),      
            StructField("max_w1", LongType(), True)
    ])


    expected_df = spark_session.createDataFrame(data = expected_data, schema =expected_schema)

    assert actual_df.collect() == expected_df.collect()
    assert actual_df.schema == expected_df.schema