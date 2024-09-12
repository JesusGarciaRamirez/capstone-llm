import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    answers_df = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json")
    questions_df = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json")
    # Explode the 'items' array to create a new row for each element in 'items'
    exploded_answers_df = answers_df.select(sf.explode(answers_df.items).alias("item"))

    # Select the 'body' field from the exploded 'item' struct
    answers_body_df = exploded_answers_df.select(
        sf.col("item.body").alias("answer_body"),
        "item.question_id"
        )

    exploded_questions_df = questions_df.select(sf.explode(questions_df.items).alias("item"))

    # Select the 'body' field from the exploded 'item' struct
    questions_body_df = exploded_questions_df.select(
            sf.col("item.body").alias("question_body"),
            "item.question_id"
        )
    clean_df = answers_body_df.join(questions_body_df, on="question_id")
    df_with_concat = (clean_df
                    .withColumn("body", sf.concat("question_body", sf.lit(" "), "answer_body"))
                    .select("body")
                )

    # Repartition to create separate files (1 row per partition)
    df_partitioned = df_with_concat.repartition(df_with_concat.count())  # repartitioning to have 1 row per partition (optional)

    # Save the DataFrame as JSON files in the directory
    df_partitioned.write.mode("overwrite").json(f"s3a://dataminded-academy-capstone-llm-data-us/cleaned/{tag}-MJ/")
    pass

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
