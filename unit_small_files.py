import sys, datetime, boto3, logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def get_files(path):
    s3 = boto3.resource('s3')
    bucket_name = path.split('/')[2]
    folder_name = path.split('/')[3]
    part_name = path.split('/')[4]
    key = f"{folder_name}/{part_name}"
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=key):
        if obj.key.endswith('/'):
            continue
        files.append(obj.key)
    return files
   
def delete_files(bucket_name, files):
    s3 = boto3.client('s3')

    bucket_name = path.split('/')[2]
    
    for file_name in files:
        try:
            s3.delete_object(Bucket=bucket_name, Key=file_name)
            logging.info(f"arquivo {file_name} deletado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao deletar arquivo {path}/{file_name}. Motivo: {e}")
        
def write_catalog(glueContext, path, catalog_db, catalog_table, dyn):
    sink = glueContext.getSink(
        path = path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ano_mes_dia"],
        enableUpdateCatalog=True,
        useGlueParquetWriter=True,
        compressionType="snappy",
        transformation_ctx=f"writeData_ctx"
    )
    
    sink.setCatalogInfo(catalogDatabase=catalog_db,catalogTableName=catalog_table)
    sink.setFormat("glueparquet")
    sink.writeFrame(dyn)

def run(glueContext, spark, path):
    try:
        df = glueContext.create_dynamic_frame.from_options(connection_type="parquet", connection_options={'paths': [path]})
        partitioned_df=df.toDF().withColumn('ano_mes_dia',lit(20230403)).repartition('ano_mes_dia')
        partitioned_dynamic_df=DynamicFrame.fromDF(partitioned_df,glueContext,"partitioned_df")
        
        s3_path="s3://apigtwy-glue-database/apigtwy_transf_flat"
        write_catalog(glueContext, s3_path,'apigtwy_database_flat', 'apigtwy_table_flat', partitioned_dynamic_df)
        logging.info("arquivo condensado OK")
    except Exception as e:
        logging.error(f"erro ao consolidar arquivo. Motivo: {e}")
        raise

if __name__ == "__main__":
    partc="ano_mes_dia=20230403"
    path = "s3://apigtwy-glue-database/apigtwy_transf_flat"
    full_path = f"{path}/{partc}"
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    files = get_files(full_path)
    run(glueContext, spark, path)
    bucket_name = path.split('/')[2]
    delete_files(bucket_name, files)
    job.commit()    
