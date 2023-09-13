from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lower, lit
from config import INPUT_PATH, OUTPUT_PATH, CHECKPOINT_PATH

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_with_schema(spark, schema, input_path):
    csvDF = spark \
            .readStream \
            .schema(schema) \
            .csv(input_path)
    return csvDF

def process_dataframe(dataframe):
    df_lower_name = dataframe.withColumn("name", lower(dataframe["name"]))
    df_with_city = df_lower_name.withColumn("city", lit("taipei"))
    return df_with_city.coalesce(1)

def write_stream_to_output(dataframe, checkpoint_path, output_path):
    query = dataframe.writeStream \
        .format("json") \
        .option("checkpointLocation", checkpoint_path) \
        .start(output_path)
    return query

def main():
    spark = create_spark_session("CSVProcessing")
    
    userSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    streaming_df = read_csv_with_schema(spark, userSchema, INPUT_PATH)
    
    streaming_df_processed = process_dataframe(streaming_df)
    
    query = write_stream_to_output(streaming_df_processed, CHECKPOINT_PATH, OUTPUT_PATH)
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
