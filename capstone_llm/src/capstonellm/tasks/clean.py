import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    llm_bucket = "dataminded-academy-capstone-llm-data-us"
    questions = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/questions.json")

    df = questions.withColumn("question", sf.explode("items"))
    df = df.select("question.*")
    df = df.select("question_id", "body", "title", "link").withColumnRenamed(
        "body", "question"
    )

    answers = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/answers.json")

    dfa = answers.withColumn("answer", sf.explode("items"))
    dfa = dfa.select("answer.*")
    dfa = dfa.select("question_id", "answer_id", "body").withColumnRenamed(
        "body", "answer"
    )

    joined = df.join(dfa, "question_id")
    joined = joined.withColumn("question_id", sf.col("question_id").cast("string"))
    joined = joined.withColumn("answer_id", sf.col("answer_id").cast("string"))
    joined.show()

    count = joined.count()
    joined.repartition(count).write.mode("overwrite").json(
        f"s3a://{llm_bucket}/cleaned/{tag}-MJ/"
    )

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
