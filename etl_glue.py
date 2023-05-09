import sys
import boto3
import pytest
import json
from datetime import datetime
from moto import mock_glue
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, LongType
from pyspark.sql.functions import from_json, col
import src.sot_etl as sot

def get_apilogs_sot_schema():
    schema = StructType([
        StructField("nom_rpst_api_gtwy",StringType(),True),
        StructField("num_cont_clod_pubi",StringType(),True),
        StructField("cod_idef_crrl_requ_api",StringType(),True),
        StructField("cod_idef_crrl_requ_clod_pubi",StringType(),True),
        StructField("cod_idef_rede_virt_clod_pubi",StringType(),True),
        StructField("cod_idef_pont_fina_rede_clod",StringType(),True),
        StructField("cod_idef_link_rede_clod_pubi",StringType(),True),
        StructField("cod_rspa_itgr_api_gtwy",IntegerType(),True),
        StructField("txt_mens_erro_itgr_api_gtwy",StringType(),True),
        StructField("qtd_temp_rspa_itgr_api_gtwy",IntegerType(),True),
        StructField("nom_url_api_gtwy",StringType(),True),
        StructField("cod_api_gtwy",StringType(),True),
        StructField("nom_esta_impl",StringType(),True),
        StructField("nom_rota_api_gtwy",StringType(),True),
        StructField("nom_base_url_api",StringType(),True),
        StructField("cod_rcur_unic_api",StringType(),True),
        StructField("nom_rcur_api",StringType(),True),
        StructField("nom_meto_api",StringType(),True),
        StructField("cod_requ_api",StringType(),True),
        StructField("num_ende_ip_clie_orig",StringType(),True),
        StructField("nom_cabe_usua_aget",StringType(),True),
        StructField("cod_tipo_prco_segc",StringType(),True),
        StructField("nom_prco_conx",StringType(),True),
        StructField("cod_rspa_tran_api_gtwy",IntegerType(),True),
        StructField("txt_mens_erro_tran_api_gtwy",StringType(),True),
        StructField("qtd_temp_rspa_tran_api_gtwy",IntegerType(),True),
        StructField("num_tama_pyld_rspa_api",IntegerType(),True),
        StructField("cod_idef_clie_gerd",StringType(),True),
        StructField("cod_tipo_emio_autr",StringType(),True),
        StructField("txt_idef_apli_csud",StringType(),True),
        StructField("cod_chav_pubi_assi",StringType(),True),
        StructField("cod_idt_tokn_aces_apli_autz",StringType(),True),
        StructField("cod_idef_orga_autz",StringType(),True),
        StructField("cod_rspa_autz_api_gtwy",IntegerType(),True),
        StructField("txt_mens_erro_autz_api_gtwy",StringType(),True),
        StructField("qtd_temp_rspa_autz_api_gtwy",IntegerType(),True),
        StructField("cod_requ_autz_api_gtwy",StringType(),True),
        StructField("dat_hor_tran_requ_gtwy",TimestampType(),True),
        StructField("ano_mes_dia",IntegerType(),True)
    ])

    return schema

@mock_glue
def test_schema_fake():
    # Start the Moto Glue mock
    glue = boto3.client('glue', region_name='us-east-1')

    # Create a test database
    glue.create_database(DatabaseInput={'Name': 'test_database'})

    # Create a test table with a schema
    database_name = 'test_database'
    table_name = 'test_table'
    schema = [
        {
            'Name': 'column1',
            'Type': 'string'
        },
        {
            'Name': 'column2',
            'Type': 'int'
        }
    ]
    glue.create_table(
        DatabaseName=database_name,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': schema,
                'Location': 's3://test_bucket/test_prefix/'
            }
        }
    )

    response = glue.get_table(DatabaseName=database_name, Name=table_name)
    fetched_schema = response['Table']['StorageDescriptor']['Columns']
    
    assert fetched_schema == schema


@pytest.fixture(scope="module", autouse=True)
def glue_context():
    sys.argv.append('--JOB_NAME')
    sys.argv.append('test_count')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    context = GlueContext(SparkSession.builder.getOrCreate())
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    yield(context)

    job.commit()


def read_file_local():
    log_schema = StructType([
        StructField("messageType", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("logGroup", StringType(), True),
        StructField("logStream", StringType(), True),
        StructField("subscriptionFilter", ArrayType(StringType()), True),
        StructField("logEvents", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("message", StringType(), True),
            ])
        ), True)
    ])

    spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext

    path = "/home/glue_user/workspace/src/sot/tests/sample/logs.json"

    json_file = sc.textFile(path)

    json_rdd = json_file.flatMap(lambda x: x.split("}{"))

    json_rdd = json_rdd.map(lambda x: x + "}" if not x.endswith("}") else x)
    json_rdd = json_rdd.map(lambda x: "{" + x if not x.startswith("{") else x)

    json_df = spark.createDataFrame(json_rdd.map(lambda x: (x,)), ["json"])

    parsed_df = json_df.select(from_json(col("json"), log_schema).alias("parsed"))

    logs_df = parsed_df.select(
        "parsed.messageType",
        "parsed.owner",
        "parsed.logGroup",
        "parsed.logStream",
        "parsed.subscriptionFilter",
        "parsed.logEvents"
    )

    return logs_df

def test_schema(glue_context):

    logs_df = read_file_local()
    
    schemed_df = sot.apply_schema(logs_df)

    message_schema = StructType().add("message", sot.message_schema())

    assert schemed_df.schema == message_schema

def test_transformed(glue_context):

    logs_df = read_file_local()
    
    schemed_df = sot.apply_schema(logs_df)

    transformed_dnf = sot.transform_flat(glue_context, schemed_df)

    partc = int(datetime.now().strftime("%Y%m%d"))

    trf_partc = [row['ano_mes_dia'] for row in transformed_dnf.toDF().
        select('ano_mes_dia').distinct().collect()][0]

    assert trf_partc == partc

def test_transformed_schema(glue_context):

    logs_df = read_file_local()
    
    schemed_df = sot.apply_schema(logs_df)

    transformed_dnf = sot.transform_flat(glue_context, schemed_df)

    assert transformed_dnf.toDF().schema == get_apilogs_sot_schema()

def test_read_file():
    logs_df = read_file_local()

    assert logs_df.count() == 30
