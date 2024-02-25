from pyspark.sql import SparkSession
from spark_utils import SparkUtils

class FileUtils():

    def convert_csv_to_parquet(spark:SparkSession,load_config):
        input_df_list = []
        dest_paths = [i for i in load_config['dest'].values()]

        for item in load_config['source'].items():
            input_df_list.append(SparkUtils.read_files(spark,'csv',item[1],{'header':'true'}))
      
        for i in range(len(input_df_list)):
            SparkUtils.write_files(input_df_list[i],'parquet',dest_paths[i])

    def deduplicate(spark:SparkSession,dedup_config):
        input_df_list = []
        source_paths= [i for i in dedup_config['source'].values()]
        dest_paths= [i for i in dedup_config['dest'].values()]

        for i in range(len(source_paths)):
            input_df_list.append(SparkUtils.read_files(spark,'parquet',source_paths[i]))
    
        for i in range(len(input_df_list)):
            input_df_list[i].show(truncate=False)
            SparkUtils.write_files(SparkUtils.dedup_df(input_df_list[i]),'parquet',dest_paths[i])

