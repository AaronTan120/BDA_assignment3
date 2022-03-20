from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window


def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    col = split(sdf['value'], ',')

    #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf

if __name__ == "__main__":
 
    spark = SparkSession.builder \
               .appName("KafkaWordCount") \
               .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    #Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    #Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema) \
        .select("topic","author","content","timestamp")


    #Create user defined function to get word len
    strlen = spark.udf.register("wordLengthString", lambda x: len(x))

    #Select content column and split them into words while adding a timestamp
    #Executes wordLengthString UDF in the where function and returns words with length more than 2
    #This eliminates common words such as "a", "to", "he", "the", etc.
    words = lines.select( 
       explode(
           split("content", " ")
       ).alias("word"), "timestamp"
    ).where("wordLengthString(word) > 3")


    #creates 2 minute windows of words with a trigger of 1 minute
    #order by window and count in ascending order
    windowedWords = words \
        .groupBy(
            window(lines.timestamp, "2 minutes", "1 minutes"),
            words.word).count().orderBy('window', "count", ascending = False)


    #creates 2 minute windows of authors with a trigger of 1 minute
    #order by window and count in ascending order
    windowedUsers = lines \
        .groupBy(
            window(lines.timestamp, "2 minutes", "1 minutes"),
            lines.author).count().orderBy('window', "count", ascending = False)

    #start streaming top 10 users output in console
    userContents = windowedUsers.writeStream.queryName("WriteContent_topusers") \
        .outputMode("complete") \
        .format("console") \
        .option("numRows",10) \
        .start()

    #start streming top 10 words output in console
    wordsContents = windowedWords.writeStream.queryName("WriteContent_topwords") \
        .outputMode("complete") \
        .format("console") \
        .option("numRows",10) \
        .start()
    
    #wait for the incoming messages for all streams
    spark.streams.awaitAnyTermination()


