from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from spark_utils import SparkUtils

def aggregate(spark:SparkSession,aggregate_config):
    source_files = aggregate_config['source']
    dest_file_root = aggregate_config['dest']['destintion_path']
    sales_df = SparkUtils.read_files(spark,'parquet',source_files['source_path_sales'])
    stores_df = SparkUtils.read_files(spark,'parquet',source_files['source_path_stores'])
    items_df = SparkUtils.read_files(spark,'parquet',source_files['source_path_items'])
    cat_df = SparkUtils.read_files(spark,'parquet',source_files['source_path_categories'])
    store_sales_agg(sales_df,items_df,cat_df,dest_file_root)

def store_sales_agg(sales_df:DataFrame,items_df:DataFrame,cat_df:DataFrame,dest_file_root:str):
    
    common_df = sales_df.join(items_df,sales_df.item_id == items_df.item_id) \
        .join(cat_df,items_df.item_category_cd == cat_df.item_category_cd) \
        .select('store_id',sales_df.item_id,'category_name','item_price')
    
    common_df.cache()

    store_sales_agg_df = common_df.groupBy('store_id').agg(f.sum('item_price').alias('total_sales')) \
        .orderBy(f.col('total_sales').desc())
    
    store_foot_fall_agg = common_df.select('store_id','item_id').distinct().groupBy('store_id').agg(f.count('item_id').alias('total_items_sold')) \
        .orderBy(f.col('total_items_sold').desc())
    

    window_spec = Window.partitionBy('store_id').orderBy(f.col('count').desc())
    store_cat_sales_agg = common_df.groupBy('store_id','category_name') \
                        .agg(f.count('category_name').alias('count')) \
                        .withColumn('rank',f.row_number().over(window_spec)) \
                        .filter(f.col('rank') <= 3) \
                        .select('store_id','category_name','rank')
    
    
    dest_file_path1 = dest_file_root + '/store_sales_agg'
    dest_file_path2 = dest_file_root + '/store_with_top_cat'
    dest_file_path3 = dest_file_root + '/store_with_most_items_sold'

    # store_sales_agg_df.show(truncate=False)
    # store_cat_sales_agg.show(truncate=False)
    # store_foot_fall_agg.show(truncate=False)

    SparkUtils.write_files(store_sales_agg_df,'parquet',dest_file_path1)
    SparkUtils.write_files(store_cat_sales_agg,'parquet',dest_file_path2)
    SparkUtils.write_files(store_cat_sales_agg,'parquet',dest_file_path3)

def stores_With_sals_aggregated(sales_df:DataFrame,items_df:DataFrame,dest_file_root:str):
    
    dest_file_path = dest_file_root + '/stores_With_sals_aggregated'

    df = sales_df.join(items_df,sales_df.item_id == items_df.item_id) \
        .select('store_id','item_price') \
        .groupBy('store_id').agg(f.sum('item_price').alias('total_sales')) \
        .orderBy(f.col('total_sales').desc())
    
    SparkUtils.write_files(df,'parquet',dest_file_path)



