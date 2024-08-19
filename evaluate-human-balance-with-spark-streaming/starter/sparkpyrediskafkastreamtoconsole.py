from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, split
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
)


# Define schemas
redisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", FloatType()),
                    ]
                )
            ),
        ),
    ]
)

customerRecordsSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)

# Initial Spark config
spark = SparkSession.builder.appName("customer-redis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Initial streaming DF
redisRawStreamingDF = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Decoding json and creting a view
redisStreamingDF = redisRawStreamingDF.selectExpr("cast(value as string) value")
redisStreamingDF.withColumn("value", from_json("value", redisSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisSortedSet")

# Decoding customer column
zSetEntriesEncodedStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string")
)
zSetDecodedEntriesStreamingDF.withColumn(
    "customer", from_json("customer", customerRecordsSchema)
).select(col("customer.*")).createOrReplaceTempView("CustomerRecords")

# Select only the email and birthday fields that aren't `null`
emailAndBirthDayStreamingDF = spark.sql(
    "select * from CustomerRecords where email is not null AND birthDay is not null"
)

# Converting the field to get the birth year
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)

# Streaming
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
