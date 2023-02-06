import sys, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import explode,col,from_json,input_file_name,struct,lit

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

name="job1"
#sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(name)

s3_path="s3://api-log-bkt/raw_data_glue/2021/11/02/"

logsDynf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[s3_path],
        "compressionType": "gzip",
        "recurse": True
    },
    format="json"
)

messageSchema = StructType([
    StructField('aws_account_id', StringType(), False),
    StructField('apigw', StructType([
        StructField('apigw_id', StringType(), False),
        StructField('endpoint_url', StringType(), False),
        StructField('stage', StringType(), False),
        StructField('context_path', StringType(), False),
        StructField('resource_path', StringType(), False),
        StructField('resource_id', StringType(), False),
        StructField('http_method', StringType(), False),
        StructField('base_path_matched', StringType(), False)
    ])),
    StructField('transaction', StructType([
        StructField('request', StructType([
            StructField('request_id', StringType(), False),
            StructField('request_time', StringType(), False),
            StructField('source_ip', StringType(), False),
            StructField('user_agent', StringType(), False),
            StructField('tls_version', StringType(), False),
            StructField('protocol', StringType(), False)
        ])),
        StructField('response', StructType([
            StructField('status', StringType(), False),
            StructField('error', StringType(), False),
            StructField('latency', StringType(), False)
        ]))
    ])),
    StructField('integration', StructType([
        StructField('response', StructType([
            StructField('status', StringType(), False),
            StructField('error', StringType(), False),
            StructField('latency', StringType(), False)
        ]))
    ])),
    StructField('tracing', StructType([
        StructField('correlation_id', StringType(), False),
        StructField('x-ray-trace', StringType(), False)
    ])),
    StructField('network', StructType([
        StructField('vpc_id', StringType(), False),
        StructField('vpce_id', StringType(), False),
        StructField('vpc_link', StringType(), False)
    ])),
    StructField('authorizer', StructType([
        StructField('identity', StructType([
            StructField('client_id', StringType(), False),
            StructField('type', StringType(), False),
            StructField('app_id', StringType(), False),
            StructField('kid', StringType(), False)
        ])),
        StructField('response', StructType([
            StructField('status', StringType(), False),
            StructField('error', StringType(), False),
            StructField('latency', StringType(), False),
            StructField('endpoint_id', StringType(), False)
        ]))
    ]))
])

ogs_df = logsDynf.toDF()

msg_df = logs_df.withColumn("logEvents_explode",explode("logEvents"))\
    .withColumn("message",from_json(col("logEvents_explode.message"),messageSchema))\
    .drop("messageType")\
    .drop("owner")\
    .drop("logStream")\
    .drop("logGroup")\
    .drop("logEvents")\
    .drop("subscriptionFilter")\
    .drop("logEvents_explode")

transformed = msg_df.select(
    msg_df["message.aws_account_id"].alias("num_cont_clod_pubi"),
    struct(
        msg_df["message.apigw.endpoint_url"].alias("txt_cami_api"),
        msg_df["message.apigw.apigw_id"].alias("cod_api_gtwy"),
        msg_df["message.apigw.stage"].alias("nom_esta_impl"),
        msg_df["message.apigw.context_path"].alias("nom_cotx_api"),
        msg_df["message.apigw.base_path_matched"].alias("nom_base_url_api"),
        msg_df["message.apigw.resource_id"].alias("cod_rcur_unic_api"),
        msg_df["message.apigw.http_method"].alias("nom_meto_api"),
        msg_df["message.apigw.resource_path"].alias("nom_rcur_api")
    ).alias("txt_estr_api_gtwy"),
    struct(
        struct(
            msg_df["message.transaction.request.request_id"].alias("cod_req_api"),
            msg_df["message.transaction.request.request_time"].alias("dat_hor_tran_requ_gtwy"),
            msg_df["message.transaction.request.source_ip"].alias("cod_ende_prco_intt_requ"),
            msg_df["message.transaction.request.user_agent"].alias("nom_cabe_usua_aget"),
            msg_df["message.transaction.request.tls_version"].alias("nom_tipo_prco_segc"),
            msg_df["message.transaction.request.protocol"].alias("nom_prco_conx")
        ).alias("txt_estr_tran_requ_gtwy"),
        struct(
            msg_df["message.transaction.response.status"].alias("cod_rspa_requ_gtwy"),
            msg_df["message.transaction.response.error"].alias("txt_mens_erro_gtwy"),
            msg_df["message.transaction.response.latency"].cast("int").alias("qtd_mlso_temp_rspa")
        ).alias("txt_estr_tran_rspa_gtwy")
    ).alias("txt_estr_tran_gtwy"),
    struct(
        struct(
            msg_df["message.integration.response.status"].alias("cod_rspa_requ_gtwy"),
            msg_df["message.integration.response.error"].alias("txt_mens_erro_apli_gtwy"),
            msg_df["message.integration.response.latency"].cast("int").alias("qtd_mlso_temp_rspa")
        ).alias("txt_estr_itgr_rspa_gtwy")
    ).alias("txt_estr_itgr_gtwy"),
    struct(
        msg_df["message.tracing.correlation_id"].alias("cod_idef_crrl_requ_api"),
        msg_df["message.tracing.x-ray-trace"].alias("cod_idef_crrl_requ_clod_pubi")
    ).alias("txt_estr_rtmt_gtwy"),
    struct(
        msg_df["message.network.vpc_id"].alias("cod_idef_rede_virt_clod_pubi"),
        msg_df["message.network.vpce_id"].alias("cod_idef_pont_fina_rede_clod"),
        msg_df["message.network.vpc_link"].alias("cod_idef_link_rede_clod_pubi"),
    ).alias("txt_estr_rede_gtwy"),
    struct(
        struct(
            msg_df["message.authorizer.identity.client_id"].alias("cod_idef_clie_gerd"),
            msg_df["message.authorizer.identity.type"].alias("cod_tipo_emio_autr"),
            msg_df["message.authorizer.identity.app_id"].alias("txt_idef_api_csud"),
            msg_df["message.authorizer.identity.kid"].alias("cod_chav_pubi_assi")
        ).alias("txt_estr_iden_autz"),
        struct(
            msg_df["message.authorizer.response.status"].alias("cod_rspa_requ_gtwy"),
            msg_df["message.authorizer.response.error"].alias("txt_mens_erro_apli_gtwy"),
            msg_df["message.authorizer.response.latency"].cast("int").alias("qtd_mlso_temp_rspa"),
            msg_df["message.authorizer.response.endpoint_id"].alias("cod_rpsa_requ_pont_fina_autz")
        ).alias("txt_estr_rspa_autz")
    ).alias("txt_estr_autz_gtwy")
)

part = 20211102

transformed = transformed.withColumn("ano_mes_dia",lit(part))

# transformed.show(1)

# transformed.printSchema()

transformed_dyn = DynamicFrame.fromDF(transformed, glueContext, "transformed_df")

s3_destination="s3://apigtwy-glue-database/apigtwy_transformed_logs/"

glueContext.write_dynamic_frame.from_options(
    frame = transformed_dyn, 
    connection_type = "s3", 
    connection_options = {"path": s3_destination, "partitionKeys": ["ano_mes_dia"]}, 
    format = "json"
)

#auto create partition
'''
part = 20191223

transformed = transformed.withColumn("ano_mes_dia",lit(part))

transformed_dyn = DynamicFrame.fromDF(transformed, glueContext, "transformed_df")

s3_destination="s3://apigtwy-glue-database/apigtwy_transformed_logs/"

write_s3_json = glueContext.getSink(
    path = s3_destination,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["ano_mes_dia"],
    enableUpdateCatalog=True,
    transformation_ctx="writeData_ctx"
)

write_s3_json.setCatalogInfo(catalogDatabase="apigtwy_database",catalogTableName="apigtwy_table")
write_s3_json.setFormat("json")
write_s3_json.writeFrame(transformed_dyn)
'''
