import sys, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def get_apilogs_sot_schema():
    schema = StructType([
        StructField("nom_rpst_api_gtwy",StringType(),False),
        StructField("num_cont_clod_pubi",StringType(),False),
        StructField("cod_idef_crrl_requ_api",StringType(),False),
        StructField("cod_idef_crrl_requ_clod_pubi",StringType(),False),
        StructField("cod_idef_rede_virt_clod_pubi",StringType(),False),
        StructField("cod_idef_pont_fina_rede_clod",StringType(),False),
        StructField("cod_idef_link_rede_clod_pubi",StringType(),False),
        StructField("cod_rspa_itgr_api_gtwy",IntegerType(),False),
        StructField("txt_mens_erro_itgr_api_gtwy",StringType(),False),
        StructField("qtd_temp_rspa_itgr_api_gtwy",IntegerType(),False),
        StructField("nom_url_api_gtwy",StringType(),False),
        StructField("cod_api_gtwy",StringType(),False),
        StructField("nom_esta_impl",StringType(),False),
        StructField("nom_rota_api_gtwy",StringType(),False),
        StructField("nom_base_url_api",StringType(),False),
        StructField("cod_rcur_unic_api",StringType(),False),
        StructField("nom_rcur_api",StringType(),False),
        StructField("nom_meto_api",StringType(),False),
        StructField("cod_requ_api",StringType(),False),
        StructField("num_ende_ip_clie_orig",StringType(),False),
        StructField("nom_cabe_usua_aget",StringType(),False),
        StructField("cod_tipo_prco_segc",StringType(),False),
        StructField("nom_prco_conx",StringType(),False),
        StructField("cod_rspa_tran_api_gtwy",IntegerType(),False),
        StructField("txt_mens_erro_tran_api_gtwy",StringType(),False),
        StructField("qtd_temp_rspa_tran_api_gtwy",IntegerType(),False),
        StructField("num_tama_pyld_rspa_api",IntegerType(),False),
        StructField("cod_idef_clie_gerd",StringType(),False),
        StructField("cod_tipo_emio_autr",StringType(),False),
        StructField("txt_idef_apli_csud",StringType(),False),
        StructField("cod_chav_pubi_assi",StringType(),False),
        StructField("cod_idt_tokn_aces_apli_autz",StringType(),False),
        StructField("cod_idef_orga_autz",StringType(),False),
        StructField("cod_rspa_autz_api_gtwy",IntegerType(),False),
        StructField("txt_mens_erro_autz_api_gtwy",StringType(),False),
        StructField("qtd_temp_rspa_autz_api_gtwy",IntegerType(),False),
        StructField("cod_requ_autz_api_gtwy",StringType(),False),
        StructField("dat_hor_tran_requ_gtwy",TimestampType(),False),
        StructField("ano_mes_dia",IntegerType(),False)
    ])

    return schema

def run(glueContext, spark, source_path, dest_path):
    num_big_files=1
    schema = get_apilogs_sot_schema()
    
#     small_files_df = spark.read.schema(schema).format("parquet")\
#             .withColumn("file_name", input_file_name()).load(source_path)

    df = spark.read.parquet(source_path) \
            .withColumn("filename", input_file_name())
    
    file_names_df = df.select('filename').distinct()
    
    file_names = [row["filename"].split('/')[-1] for row in file_names_df.collect()]
    
    df.drop('filename')
    
#     return file_names
    
    small_files_df.repartition(num_big_files).write.option("dataChange","false")\
           .format("parquet")\
           .mode("overwrite")\
           .save(dest_path)
    
    

partc= "20230502"
source_path = f"/home/glue_user/s3_data/ano_mes_dia={partc}/"
dest_path = f"/home/glue_user/s3_data/big_files/ano_mes_dia={partc}/"

glueContext = GlueContext(sc)
spark = glueContext.spark_session

filenames = run(glueContext, spark, source_path, dest_path)
