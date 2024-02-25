from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as f

class SparkUtils():

    @staticmethod
    def read_files(spark:SparkSession,format:str,path:str,options:map={}):
        return spark.read.format(format).options(**options).load(path=path)

    @staticmethod
    def write_files(df:DataFrame,format:str,path:str):
        df.write.mode('overwrite').format(format).save(path=path)
    
    @staticmethod
    def dedup_df(df:DataFrame):
        return df.dropna().dropDuplicates()