import logging
import argparse
import json
from pyspark.sql import SparkSession
from file_format_conversion import FileUtils
from aggregate import aggregate


logger = logging.getLogger("jcp_poc")
logger.setLevel(logging.INFO)


arguments = ["--config_file_path",
             "--process_name",
             "--local_mode"
             ]

def parse_args():
    """Parse command line."""
    parser = argparse.ArgumentParser()
    for argument in arguments:
        parser.add_argument(argument)
    args = parser.parse_args()
    logger.info(f"Input arguments dict: {args}")
    return args

def _get_spark_session(app_name):
    return (SparkSession.builder.appName(app_name)).getOrCreate()

def main():
    args = parse_args()
    common_cfg = json.load(open(args.config_file_path))
    spark = _get_spark_session(args.process_name)
    process = args.process_name
    if args.local_mode == 'y':
        spark.sparkContext.setSystemProperty("hadoop.home.dir","C:/hadoop/bin")
        spark.sparkContext.setLogLevel('ERROR')

    if process.lower() == 'load':
        FileUtils.convert_csv_to_parquet(spark,common_cfg[process.lower()])
    elif process.lower() == 'deduplicate':
        FileUtils.deduplicate(spark,common_cfg[process.lower()])
    elif process.lower() == 'aggregate':
        aggregate(spark,common_cfg[process.lower()])



if __name__ == '__main__':
    main()
