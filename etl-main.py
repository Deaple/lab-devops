import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import explode, col, from_json, struct, lit

'''
exec
export S3_SRC='s3://api-log-bkt/raw_data_glue_jan/'
export S3_DEST='s3://apigtwy-glue-database/apigtwy_transformed_logs/'
export DB='apigtwy_database'
export TABLE='apigtwy_table'

python3 src/etl-main.py \
	--JOB_NAME='job1'\
	--S3_SOURCE=$S3_SRC \
	--S3_DESTINATION=$S3_DEST \
	--DATABASE_NAME=$DB \
	--TABLE_NAME=$TABLE		
'''


def read_files_from_s3(glue_context, path):
    logs_dynf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [path],
            "compressionType": "gzip",
            "recurse": True
        },
        format="json",
        transformation_ctx="leitura_s3"
    )

    return logs_dynf


def getMessageSchema():
    return StructType([
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


def write_to_s3(glue_context, dynamic_frame, path):
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": path, "partitionKeys": ["ano_mes_dia"]},
        format="json",
        transformation_ctx="escrita_s3"
    )


def write_to_catalog(
        glue_context, dynamic_frame, s3_path, database_name, table_name):
    write_s3_json = glue_context.getSink(
        path=s3_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ano_mes_dia"],
        enableUpdateCatalog=True,
        transformation_ctx="escrita_s3"
    )

    write_s3_json.setCatalogInfo(catalogDatabase=database_name,
                                 catalogTableName=table_name)
    write_s3_json.setFormat("json")
    write_s3_json.writeFrame(dynamic_frame)


def transform_logs(glue_context, dynamic_frame, partition):
    messageSchema = getMessageSchema()
    # ano_mes_dia_partition = int(datetime.now().strftime("%Y%m%d"))
    ano_mes_dia_partition = int(partition.replace('/',''))
    logs_df = dynamic_frame.toDF()
    msg_df = logs_df.withColumn("logs_exp", explode("logEvents"))\
        .withColumn("message", from_json(
            col("logs_exp.message"), messageSchema))\
        .drop("messageType")\
        .drop("owner")\
        .drop("logStream")\
        .drop("logGroup")\
        .drop("logEvents")\
        .drop("subscriptionFilter")\
        .drop("logs_exp")

    transformed_df = msg_df.select(
        msg_df["message.aws_account_id"]
        .alias("num_cont_clod_pubi"),
        struct(
            msg_df["message.apigw.endpoint_url"]
            .alias("txt_cami_api"),
            msg_df["message.apigw.apigw_id"]
            .alias("cod_api_gtwy"),
            msg_df["message.apigw.stage"]
            .alias("nom_esta_impl"),
            msg_df["message.apigw.context_path"]
            .alias("nom_cotx_api"),
            msg_df["message.apigw.base_path_matched"]
            .alias("nom_base_url_api"),
            msg_df["message.apigw.resource_id"]
            .alias("cod_rcur_unic_api"),
            msg_df["message.apigw.http_method"]
            .alias("nom_meto_api"),
            msg_df["message.apigw.resource_path"]
            .alias("nom_rcur_api")
        ).alias("txt_estr_api_gtwy"),
        struct(
            struct(
                msg_df["message.transaction.request.request_id"]
                .alias("cod_requ_api"),
                msg_df["message.transaction.request.request_time"]
                .alias("dat_hor_tran_requ_gtwy"),
                msg_df["message.transaction.request.source_ip"]
                .alias("cod_ende_prco_intt_requ"),
                msg_df["message.transaction.request.user_agent"]
                .alias("nom_cabe_usua_aget"),
                msg_df["message.transaction.request.tls_version"]
                .alias("nom_tipo_prco_segc"),
                msg_df["message.transaction.request.protocol"]
                .alias("nom_prco_conx")
            ).alias("txt_estr_tran_requ_gtwy"),
            struct(
                msg_df["message.transaction.response.status"]
                .alias("cod_rspa_requ_gtwy"),
                msg_df["message.transaction.response.error"]
                .alias("txt_mens_erro_gtwy"),
                msg_df["message.transaction.response.latency"]
                .alias("qtd_mlso_temp_rspa")
            ).alias("txt_estr_tran_rspa_gtwy")
        ).alias("txt_estr_tran_gtwy"),
        struct(
            struct(
                msg_df["message.integration.response.status"]
                .alias("cod_rspa_requ_gtwy"),
                msg_df["message.integration.response.error"]
                .alias("txt_mens_erro_apli_gtwy"),
                msg_df["message.integration.response.latency"]
                .alias("qtd_mlso_temp_rspa")
            ).alias("txt_estr_itgr_rspa_gtwy")
        ).alias("txt_estr_itgr_gtwy"),
        struct(
            msg_df["message.tracing.correlation_id"]
            .alias("cod_idef_crrl_requ_api"),
            msg_df["message.tracing.x-ray-trace"]
            .alias("cod_idef_crrl_requ_clod_pubi")
        ).alias("txt_estr_rtmt_gtwy"),
        struct(
            msg_df["message.network.vpc_id"]
            .alias("cod_idef_rede_virt_clod_pubi"),
            msg_df["message.network.vpce_id"]
            .alias("cod_idef_pont_fina_rede_clod"),
            msg_df["message.network.vpc_link"]
            .alias("cod_idef_link_rede_clod_pubi"),
        ).alias("txt_estr_rede_gtwy"),
        struct(
            struct(
                msg_df["message.authorizer.identity.client_id"]
                .alias("cod_idef_clie_gerd"),
                msg_df["message.authorizer.identity.type"]
                .alias("cod_tipo_emio_autr"),
                msg_df["message.authorizer.identity.app_id"]
                .alias("txt_idef_api_csud"),
                msg_df["message.authorizer.identity.kid"]
                .alias("cod_chav_pubi_assi")
            ).alias("txt_estr_iden_autz"),
            struct(
                msg_df["message.authorizer.response.status"]
                .alias("cod_rspa_requ_gtwy"),
                msg_df["message.authorizer.response.error"]
                .alias("txt_mens_erro_apli_gtwy"),
                msg_df["message.authorizer.response.latency"]
                .alias("qtd_mlso_temp_rspa"),
                msg_df["message.authorizer.response.endpoint_id"]
                .alias("cod_requ_api")
            ).alias("txt_estr_rspa_autz")
        ).alias("txt_estr_autz_gtwy"),
        lit(ano_mes_dia_partition).alias('ano_mes_dia')
    )

    transformed_dynf = DynamicFrame.fromDF(transformed_df,
                                           glue_context, "logs_transformados")

    return transformed_dynf


def run(glueContext, day):
    # dateTimePartition = datetime.now().strftime("%Y/%m/%d")
    date_time_partition = "2023/01/" + day

    params = [
        'JOB_NAME', 'S3_SOURCE', 'S3_DESTINATION',
        'DATABASE_NAME', 'TABLE_NAME']
    args = getResolvedOptions(sys.argv, params)

    job_name = args['JOB_NAME']
    s3_source_path = args['S3_SOURCE'] + date_time_partition + '/'
    s3_destination_path = args['S3_DESTINATION']
    table_name = args['TABLE_NAME']
    database_name = args['DATABASE_NAME']

    job = Job(glueContext)

    job.init(job_name, args)

    logs_dynf = read_files_from_s3(glueContext, s3_source_path)

    transformed_logs_dynf = transform_logs(glueContext, logs_dynf, date_time_partition)

    write_to_catalog(
        glue_context=glueContext,
        dynamic_frame=transformed_logs_dynf,
        table_name=table_name,
        database_name=database_name,
        s3_path=s3_destination_path
    )

    job.commit()

if __name__ == '__main__':
    sc = SparkContext()
    glueContext = GlueContext(sc)
    
    for i in range(1,32):
        day = str(i).rjust(2,"0")
        print(f"runinng job day: {day}")
        run(glueContext,day)
        print(f"finishing job day: {day}")
