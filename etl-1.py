import sys, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col,lit,countDistinct, count, length


def read_logs_catalog(glueContext, database, table, partition):

    logs_catalog_df = glueContext.create_dynamic_frame.from_catalog(
        transformation_ctx = "read_logs_ctx",
        database=database,
        table_name=table,
        push_down_predicate=partition
    ).toDF()

    return logs_catalog_df


def read_git_s3(glueContext, path, partition):

    git_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        transformation_ctx = "read_git_ctx",
        connection_options={
            "paths":[path],
            "recurse": True
        },
        push_down_predicate=partition,
        format="csv"
    ).toDF()

    return git_df


def read_api_s3(glueContext, path, partition):

    api_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        transformation_ctx = "read_api_ctx",
        push_down_predicate=partition,
        connection_options={
            "paths":[path],
            "compressionType": "gzip",
            "recurse": True
        },
        format="json"
    ).toDF()

    return api_df


def read_sig_s3(glueContext, path, partition):

    sig_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        transformation_ctx = "read_sig_ctx",
        connection_options={
            "paths":[path],
            "recurse": True
        },
        push_down_predicate=partition,
        format="csv"
    ).toDF()

    return sig_df


def flat_logs_table(logs_catalog_df):

    log_flat_df = logs_catalog_df.select(
        logs_catalog_df.num_cont_clod_pubi.alias('num_aws_account'),
    #    logs_catalog_df.nom_rspt_api_gtwy.alias("repo_name"),
        logs_catalog_df.txt_estr_api_gtwy.\
            txt_cami_api.alias('api_gtw_url'),
        logs_catalog_df.txt_estr_api_gtwy.\
            cod_api_gtwy.alias('cod_api_gty'),
        logs_catalog_df.txt_estr_api_gtwy.\
            nom_esta_impl.alias('stage'),
        logs_catalog_df.txt_estr_api_gtwy.\
            nom_cotx_api.alias('context_path'),
        logs_catalog_df.txt_estr_api_gtwy.\
            nom_base_url_api.alias('base_path'),
        logs_catalog_df.txt_estr_api_gtwy.\
            cod_rcur_unic_api.alias('api_res_id'),
        logs_catalog_df.txt_estr_api_gtwy.\
            nom_meto_api.alias('method'),
        logs_catalog_df.txt_estr_api_gtwy.\
            nom_rcur_api.alias('resource_path'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.cod_requ_api.alias('tran_req_cod'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.\
                dat_hor_tran_requ_gtwy.alias('tran_req_datetime_request'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.\
                cod_ende_prco_intt_requ.alias('tran_req_source_ip'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.\
                nom_cabe_usua_aget.alias('tran_req_user_agent'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.\
                nom_tipo_prco_segc.alias('tran_req_tls_protocol'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_requ_gtwy.\
                nom_prco_conx.alias('tran_req_http_protocol'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_rspa_gtwy.\
                cod_rspa_requ_gtwy.alias('tran_resp_status'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_rspa_gtwy.\
                txt_mens_erro_gtwy.alias('tran_resp_error_msg'),
        logs_catalog_df.txt_estr_tran_gtwy.\
            txt_estr_tran_rspa_gtwy.\
                qtd_mlso_temp_rspa.alias('tran_resp_temp_ms'),
        logs_catalog_df.txt_estr_itgr_gtwy.\
            txt_estr_itgr_rspa_gtwy.\
                cod_rspa_requ_gtwy.alias('integ_resp_status'),
        logs_catalog_df.txt_estr_itgr_gtwy.\
            txt_estr_itgr_rspa_gtwy.\
                txt_mens_erro_apli_gtwy.alias('integ_error_msg'),
        logs_catalog_df.txt_estr_itgr_gtwy.\
            txt_estr_itgr_rspa_gtwy.\
                qtd_mlso_temp_rspa.alias('integ_resp_temp_ms'),
        logs_catalog_df.txt_estr_rtmt_gtwy.\
            cod_idef_crrl_requ_api.alias('xray_cod_request'),
        logs_catalog_df.txt_estr_rtmt_gtwy.\
            cod_idef_crrl_requ_clod_pubi.alias('xray_cod_aws'),
        logs_catalog_df.txt_estr_rede_gtwy.\
            cod_idef_rede_virt_clod_pubi.alias('vpc_id'),
        logs_catalog_df.txt_estr_rede_gtwy.\
            cod_idef_pont_fina_rede_clod.alias('vpce_id'),
        logs_catalog_df.txt_estr_rede_gtwy.\
            cod_idef_link_rede_clod_pubi.alias('vpc_link'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_iden_autz.cod_idef_clie_gerd.alias('aut_req_client_id'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_iden_autz.cod_tipo_emio_autr.alias('aut_req_provider'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_iden_autz.\
                txt_idef_api_csud.alias('aut_req_app_id'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_iden_autz.\
                cod_chav_pubi_assi.alias('aut_req_kid'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_rspa_autz.\
                cod_rspa_requ_gtwy.alias('aut_esp_status'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_rspa_autz.\
                txt_mens_erro_apli_gtwy.alias('aut_resp_error_msg'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_rspa_autz.\
                qtd_mlso_temp_rspa.alias('aut_resp_temp_ms'),
        logs_catalog_df.txt_estr_autz_gtwy.\
            txt_estr_rspa_autz.\
                cod_requ_api.alias('aut_resp_cod_req'),
        logs_catalog_df.ano_mes_dia.alias('ano_mes_dia')
    )\
    .drop('txt_estr_api_gtwy')\
    .drop('txt_estr_tran_gtwy.txt_estr_tran_requ_gtwy')\
    .drop('txt_estr_tran_gtwy.txt_estr_tran_rspa_gtwy')\
    .drop('txt_estr_tran_gtwy')\
    .drop('txt_estr_itgr_gtwy.txt_estr_itgr_rspa_gtwy')\
    .drop('txt_estr_itgr_gtwy')\
    .drop('txt_estr_rtmt_gtwy')\
    .drop('txt_estr_rede_gtwy')\
    .drop('txt_estr_autz_gtwy.txt_estr_iden_autz')\
    .drop('txt_estr_autz_gtwy.txt_estr_rspa_autz')\
    .drop('txt_estr_autz_gtwy')
    
    return log_flat_df


def calc_total_reuse(log_flat_df):
    reuse_df = log_flat_df.select("resource_path", "method", 
    "aut_req_client_id", "cod_api_gty") \
                .where(length(col("aut_req_client_id")) > 9) \
                .groupBy("method", "resource_path") \
                .agg((countDistinct("aut_req_client_id") - 1)\
                    .alias("reuso"),
                    count("*").alias("total_requisicoes"))\
                        .orderBy("reuso", ascending=True)
    return reuse_df


def transform_reuse_fact(git_df, log_df, sig_df, key):
    
    if key == "git":
        key_repo = "nom_rpst_api_gtwy"
        key_log = "repository_name"
    elif key == "api":
        key_repo = "codApiGtwy"
        key_log = "cod_api_gtwy"
    
    joined_log_git = log_df.join(git_df, [key_repo, key_log], "inner")
    
    all_joined_df = joined_log_git.join(git_df, ["sig", "sigla"], "inner")
    
    partition = int(datetime.now().strftime("%Y%m%d"))-1
    
    result_df = all_joined_df.select(
        all_joined_df.resource_path.alias('nom_rota_api'), #git/api/logs
        all_joined_df.nom_ambi_hosg.alias('nom_ambi_hosg'), #git/api
        all_joined_df.method.alias('nom_meto_chma'), #git/logs
        all_joined_df.nom_rpst_api_gtwy.alias('nom_rpst_apli'), #git/logs
        all_joined_df.int_aute_api.alias('int_aute_api'), #git/api
        all_joined_df.ind_pssu_docu_api.alias('ind_pssu_docu_api'), #dev
        all_joined_df.total_requisicoes.alias('num_requ_pont_fina_api_gtwy'), #logs
        all_joined_df.grupo_suporte.alias('nom_agru_squd_rspl_api'), #sig
        all_joined_df.nom_espf_api.alias('nom_espf_api'), #git/api
        all_joined_df.nom_vip_dese.alias('nom_vip_dese'), #git/api
        all_joined_df.nom_vip_homo.alias('nom_vip_homo'), #git/api
        all_joined_df.nom_vip_prdu.alias('nom_vip_prdu'), #git/api
        all_joined_df.sig.alias('sig'), #sig
        all_joined_df.nom_cmne.alias('nom_cmne'), #sig
        all_joined_df.nom_grup_supo.alias('nom_grup_supo'), #sig
        all_joined_df.nom_dret_cmne.alias('nom_dret_cmne'), #sig
        all_joined_df.reuso.alias('num_calc_rtlo_api'), #logs
        lit(partition).alias('ano_mes_dia_tran') #partition
    )
    
    return result_df


def write_fact_catalog(glueContext, df, s3_destination):
    dynf = DynamicFrame.fromDF(df, glueContext, "transformed_df")
    
    write_catalog = glueContext.getSink(
        path = s3_destination,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ano_mes_dia_tran"],
        enableUpdateCatalog=True,
        transformation_ctx="writeData_ctx"
    )

    write_catalog.setCatalogInfo(catalogDatabase="reuse_database",catalogTableName="reuse_table")
    write_catalog.setFormat("parquet")
    write_catalog.writeFrame(dynf)


def run(glueContext):

    params = [
        'JOB_NAME', 'S3_GIT_SOURCE', 'S3_API_SOURCE', 
        'S3_SIG_SOURCE', 'S3_LOG_SOURCE', 'S3_DESTINATION',
        'LOG_DATABASE_NAME', 'LOG_TABLE_NAME']
    args = getResolvedOptions(sys.argv, params)

    job_name = args['JOB_NAME']
    #path_logs_s3 = args['S3_LOG_SOURCE']
    path_git_s3 = args['S3_GIT_SOURCE']
    # path_apis_s3 = args['S3_API_SOURCE']
    path_sig_s3 = args['S3_SIG_SOURCE']
    s3_dest_path = args['S3_DESTINATION']
    table = args['LOG_TABLE_NAME']
    database = args['LOG_DATABASE_NAME']

    part_predict="(ano_mes_dia=20230328)"

    job = Job(glueContext)

    job.init(job_name, args)

    log_df = read_logs_catalog(glueContext, database, table, part_predict)

    git_df = read_git_s3(glueContext, path_git_s3, part_predict)
    # api_df = read_api_s3(glueContext, path_apis_s3, partition)
    sig_df = read_sig_s3(glueContext, path_sig_s3, part_predict)

    flat_logs_df = flat_logs_table(log_df)

    reuse_logs_df = calc_total_reuse(flat_logs_df)

    transformed_df = transform_reuse_fact(git_df, 
    reuse_logs_df, sig_df, "git")

    write_fact_catalog(glueContext, transformed_df, 
    s3_dest_path)

    job.commit()

if __name__ == '__main__':
    sc = SparkContext()
    glueContext = GlueContext(sc)
    run(glueContext)
