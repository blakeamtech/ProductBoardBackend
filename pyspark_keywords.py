from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, explode, col, sum
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, DoubleType
import os
from ProductHuntScraper import ProductHuntScraper  # Import the scraper class
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UploadKeyWordsToS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_KEY')) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Load the custom stopwords from a file
stopwords_path = 'stopwords.txt'  # Update this path
with open(stopwords_path, 'r') as file:
    custom_stopwords = [line.strip() for line in file.readlines()]

# Use the scraper to fetch data
scraper = ProductHuntScraper()
weekly_top_products_df = scraper.get_weekly_top_products()  # Assuming you want to scrape weekly top products
df = spark.createDataFrame(weekly_top_products_df)

# Preprocessing steps, including custom stopwords removal
tokenizer = RegexTokenizer(inputCol="Description", outputCol="tokens", pattern="\\b[\\p{L}']+[\\p{L}\\p{Pd}']*[\\p{L}]\\b|\\b[\\p{L}]\\b", gaps=False)
default_stopwords = StopWordsRemover.loadDefaultStopWords("english")  # Load default stopwords
all_stopwords = list(set(default_stopwords + custom_stopwords))  # Combine default with custom stopwords
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens", stopWords=all_stopwords)

# Continue with CountVectorizer and IDF as before
cv = CountVectorizer(inputCol="filtered_tokens", outputCol="cv_features")
idf = IDF(inputCol="cv_features", outputCol="tfidf_features")

# Assemble the pipeline
pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])

# Fit the pipeline to the dataset
model = pipeline.fit(df)

# Transform the data
result = model.transform(df)

# Extract vocabulary
vocabulary = model.stages[2].vocabulary

# Define a UDF to convert TF-IDF features to word-score pairs
def tfidf_to_word_score_pairs(features):
    indices = features.indices.tolist()
    values = features.values.tolist()
    return [(vocabulary[index], value) for index, value in zip(indices, values)]

tfidf_to_word_score_pairs_udf = udf(tfidf_to_word_score_pairs, ArrayType(StructType([
    StructField("word", StringType()),
    StructField("score", DoubleType())
])))

# Apply the UDF to create a new column with word-score pairs
result = result.withColumn("word_score_pairs", tfidf_to_word_score_pairs_udf("tfidf_features"))

# Explode the word-score pairs to individual rows, aggregate, and find top words
exploded_result = result.select(explode("word_score_pairs").alias("word_score"), "date")
word_score_df = exploded_result.select(
    col("word_score.word").alias("word"),
    col("word_score.score").alias("score"),
    "date",
)

top_words = word_score_df.groupBy("word", "date").agg(sum("score").alias("total_score")) \
            .orderBy(col("total_score").desc()).limit(10)

date = datetime.now().date()
# Define your S3 path
s3_path = f"s3a://product-hunt-keywords/{date}_data"

# Save the DataFrame to the specified S3 path in Parquet format
top_words.coalesce(1).write.mode("overwrite").json(s3_path)

# Stop the Spark session
spark.stop()