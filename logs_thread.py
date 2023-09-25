import os
import random
import string
import uuid
import copy
import threading
from faker import Faker

# Initialize Faker
faker = Faker()

raw_msg = r'''
    "id": "CHANGE_ID",
    "timestamp": CHANGE_TIMESTMP,
    "message": "{\"repository_name\": \"CHANGE_REPO_NAME\",\"aws_account_id\": \"CHANGE_OWNER\",\"apigw\": {\"endpoint_url\": \"CHANGE_GTW_ID.execute-api.us-east-1.amazonaws.com\",\"apigw_id\": \"CHANGE_GTW_ID\",\"stage\": \"CHANGE_STAGE\",\"context_path\": \"CHANGE_CONTEXT_PATH\",\"base_path_matched\": \"CHANGE_BASEPATH_MATCH\",\"resource_id\": \"CHANGE_RES_ID\",\"resource_path\": \"CHANGE_RES_PATH\",\"http_method\": \"CHANGE_METHOD\"},\"transaction\": {\"request\": {\"request_id\": \"CHANGE_REQUEST_ID\",\"request_time\": \"CHANGE_RQT_TIME\",\"source_ip\": \"CHANGE_IP_ADDR\",\"user_agent\": \"CHANGE_USER_AGENT\",\"tls_version\": \"CHANGE_TLS_VER\",\"protocol\": \"CHANGE_PROTOCOL\"},\"response\": {\"status\": \"CHANGE_TRAN_CODE\",\"error\": \"CHANGE_TRAN_ERR_MSG\",\"latency\": \"CHANGE_TRAN_LATENCY\", \"payload_length\":\"CHANGE_PAYLOAD_LEN\"}},\"tracing\": {\"correlation_id\": \"CHANGE_CORRELATION_ID\",\"x-ray-trace\": \"CHANGE_X_RAY\"},\"network\": {\"vpc_id\": \"CHANGE_VPC_ID\",\"vpce_id\": \"CHANGE_VPCE_ID\",\"vpc_link\": \"CHANGE_VPC_LINK\"},\"integration\": {\"response\": {\"status\": \"CHANGE_INT_CODE\",\"error\": \"CHANGE_INT_ERR_MSG\",\"latency\": \"CHANGE_INT_LATENCY\"}},\"authorizer\": {\"identity\": {\"client_id\": \"CHANGE_AUTH_CLIENTID\",\"type\": \"CHANGE_AUTH_TYPE\",\"app_id\": \"CHANGE_APP_ID\",\"kid\": \"CHANGE_AUTH_KID\", \"access_token\":\"CHANGE_ACCESS_TOKEN\", \"org_id\":\"CHANGE_ORG_ID\"},\"response\": {\"status\": \"CHANGE_AUTH_CODE\",\"error\": \"CHANGE_AUTH_ERR_MSG\",\"latency\": \"CHANGE_AUTH_LATENCY\",\"endpoint_id\":\"CHANGE_AUTH_ENDPOINT_ID\"}}}"
'''
raw_log = r'''
{
    "messageType": "DATA_MESSAGE",
    "owner": "CHANGE_OWNER",
    "logGroup": "/aws/apigateway/gateway-teste",
    "logStream": "CHANGE_LOG",
    "subscriptionFilter": [
        "cf-gateway-teste-aws-subscriptionFilter-123asca"
    ],
    "logEvents": [
line
    ]
}
'''


def gera_repo_name():
    sigla = ''.join(random.choices(string.ascii_lowercase, k=2)) + \
        str(random.choice(string.digits))
    words = [x.lower() for x in faker.words(2)]
    type = random.choice(['int', 'binint', 'ext', 'binext', 'tec', 'tecint', 'bff'])
    name = f'repo-{sigla}-gtw-{words[0]}-{words[1]}-{type}-aws'
    return name


def gera_api_gtw_id():
    gtw_id = ''.join(random.choices(string.ascii_lowercase +
                                    string.digits, k=10))
    return gtw_id


repo_names = []
for _ in range(11):
    repo_names.append(gera_repo_name())

api_gtw_ids = []
for _ in range(11):
    api_gtw_ids.append(gera_api_gtw_id())

repo_names.append('')

print(f"repo names list: {repo_names}")
print(f"cod gtw id names list: {api_gtw_ids}")


def random_data(data):

    CHANGE_REPO_NAME = random.choice(repo_names)
    CHANGE_ID = ''.join(random.choices('0123456789', k=13))
    CHANGE_TIMESTMP = int(''.join(random.choices('0123456789', k=12)))
    CHANGE_LOG = ''.join(random.choices('0123456789', k=12))

    CHANGE_OWNER = ''.join(random.choices('0123456789', k=12))
    CHANGE_RES_PATH = random.choice(['/products/1', '/login', '/process/100', '/pages', '/news', '/photos/661', '/movies/2909', '/users/555'])

    CHANGE_STAGE = random.choice(['prod', 'hom', 'dev'])
    CHANGE_GTW_ID = random.choice(api_gtw_ids)
    CHANGE_RES_ID = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

    CHANGE_API_PATH = faker.url() + CHANGE_STAGE + CHANGE_RES_PATH
    CHANGE_CONTEXT_PATH = CHANGE_STAGE + CHANGE_RES_PATH
    CHANGE_BASEPATH_MATCH = random.choice([CHANGE_STAGE + CHANGE_RES_PATH, '-', '(none)'])
    CHANGE_METHOD = random.choice(['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
    CHANGE_REQUEST_ID = str(uuid.uuid4())
    CHANGE_RQT_TIME = faker.date_time().strftime("%d/%b/%Y:%H:%M:%S +0000")
    CHANGE_TLS_VER = random.choice(['1.2', '1.3', '-', '(none)'])
    CHANGE_PROTOCOL = random.choice(['HTTP/1.1', 'HTTP/2'])

    CHANGE_TRAN_CODE = random.choice(['200', '201', '302'])
    CHANGE_TRAN_ERR_MSG = random.choice(['erro transaction 1', 'erro transaction 2', '-'])
    CHANGE_TRAN_LATENCY = ''.join(random.choices(string.digits, k=3))
    CHANGE_PAYLOAD_LEN = str(random.choice(['-', random.randint(1, 10000)]))

    CHANGE_INT_CODE = random.choice(['200', '201', '302', '-'])
    CHANGE_INT_ERR_MSG = random.choice(['erro integracao 1', 'erro integracao 2', '-'])
    CHANGE_INT_LATENCY = ''.join(random.choices(string.digits, k=3))

    CHANGE_VPC_ID = random.choice(['vpc-0' + ''.join(random.choices(string.hexdigits[:-6] + string.digits, k=16)), '-'])
    CHANGE_VPCE_ID = random.choice(['vpce-0' + ''.join(random.choices(string.hexdigits[:-6] + string.digits, k=16)), '-'])
    CHANGE_VPC_LINK = "vpc-link-" + ''.join(random.choice(string.digits))

    CHANGE_X_RAY = 'Root=1-' + str(uuid.uuid4())
    CHANGE_CORRELATION_ID = str(uuid.uuid4())
    CHANGE_IP_ADDR = faker.ipv4()
    CHANGE_USER_AGENT = faker.user_agent()
    uuids_client_id = ['24c7d80f-11c9-4fff-9c07-f95c88dbed5a', 'e4a01f62-8b3c-4551-b042-d8670be4706d', '9cdc5474-f643-4195-a0d1-a748e047690b', '2af8ab15-2fec-4dae-9d4b-c05fb74995e8', '2fed8cc1-57d6-41ec-bb79-90b399d69128', '947dd31b-6b01-455d-ac27-45f9d8841191', '266ddc1e-69fc-4bb8-a851-58004631f6bb', '8b342609-d1a4-4809-a485-08be88810c74', 'c7c8a5d3-e0aa-4edb-ba5f-21157d046f5f']
    uuid1 = random.choice(uuids_client_id)
    racf = ''.join(random.choices(string.digits, k=9))
    CHANGE_AUTH_CLIENTID = random.choice([str(uuid.uuid4()), uuid1, '-', racf])
    CHANGE_AUTH_TYPE = random.choice(['ISS_ISAM', 'ISS_STS', 'ISS_AZURE', 'ISS_CIAM'])
    CHANGE_AUTH_KID = random.choice(['kid.' + str(uuid.uuid4()), '-'])
    CHANGE_APP_ID = random.choice(['app-' + random.choice(string.digits), '-', '(none)'])
    CHANGE_ACCESS_TOKEN = random.choice([str(uuid.uuid4()), '-'])
    CHANGE_ORG_ID = random.choice(['org_1', 'org_2', 'org_3', 'org_4', '-', '(none)'])

    CHANGE_AUTH_CODE = random.choice(['200', '201', '302'])
    CHANGE_AUTH_ERR_MSG = random.choice(['erro authorizer 1', 'erro authorizer 2', '-', '(none)'])
    CHANGE_AUTH_LATENCY = ''.join(random.choices(string.digits, k=3))
    CHANGE_AUTH_ENDPOINT_ID = str(uuid.uuid4())

    data_changed = {
        'CHANGE_REPO_NAME': CHANGE_REPO_NAME,
        'CHANGE_ID': CHANGE_ID,
        'CHANGE_TIMESTMP': CHANGE_TIMESTMP,
        'CHANGE_LOG': CHANGE_LOG,
        'CHANGE_OWNER': CHANGE_OWNER,
        'CHANGE_RES_PATH': CHANGE_RES_PATH,
        'CHANGE_RES_ID': CHANGE_RES_ID,
        'CHANGE_STAGE': CHANGE_STAGE,
        'CHANGE_GTW_ID': CHANGE_GTW_ID,
        'CHANGE_API_PATH': CHANGE_API_PATH,
        'CHANGE_CONTEXT_PATH': CHANGE_CONTEXT_PATH,
        'CHANGE_BASEPATH_MATCH': CHANGE_BASEPATH_MATCH,
        'CHANGE_METHOD': CHANGE_METHOD,
        'CHANGE_REQUEST_ID': CHANGE_REQUEST_ID,
        'CHANGE_RQT_TIME': CHANGE_RQT_TIME,
        'CHANGE_TLS_VER': CHANGE_TLS_VER,
        'CHANGE_PROTOCOL': CHANGE_PROTOCOL,
        'CHANGE_TRAN_CODE': CHANGE_TRAN_CODE,
        'CHANGE_TRAN_ERR_MSG': CHANGE_TRAN_ERR_MSG,
        'CHANGE_TRAN_LATENCY': CHANGE_TRAN_LATENCY,
        'CHANGE_PAYLOAD_LEN': CHANGE_PAYLOAD_LEN,
        'CHANGE_INT_CODE': CHANGE_INT_CODE,
        'CHANGE_INT_ERR_MSG': CHANGE_INT_ERR_MSG,
        'CHANGE_INT_LATENCY': CHANGE_INT_LATENCY,
        'CHANGE_VPC_ID': CHANGE_VPC_ID,
        'CHANGE_VPCE_ID': CHANGE_VPCE_ID,
        'CHANGE_VPC_LINK': CHANGE_VPC_LINK,
        'CHANGE_X_RAY': CHANGE_X_RAY,
        'CHANGE_CORRELATION_ID': CHANGE_CORRELATION_ID,
        'CHANGE_IP_ADDR': CHANGE_IP_ADDR,
        'CHANGE_USER_AGENT': CHANGE_USER_AGENT,
        'CHANGE_AUTH_CLIENTID': CHANGE_AUTH_CLIENTID,
        'CHANGE_AUTH_TYPE': CHANGE_AUTH_TYPE,
        'CHANGE_AUTH_KID': CHANGE_AUTH_KID,
        'CHANGE_ACCESS_TOKEN': CHANGE_ACCESS_TOKEN,
        'CHANGE_ORG_ID': CHANGE_ORG_ID,
        'CHANGE_APP_ID': CHANGE_APP_ID,
        'CHANGE_AUTH_CODE': CHANGE_AUTH_CODE,
        'CHANGE_AUTH_ERR_MSG': CHANGE_AUTH_ERR_MSG,
        'CHANGE_AUTH_LATENCY': CHANGE_AUTH_LATENCY,
        'CHANGE_AUTH_ENDPOINT_ID': CHANGE_AUTH_ENDPOINT_ID
    }

    for key, value in data_changed.items():
        data = data.replace(key, str(value))

    return data


def rand_msg(raw_msg):
    line = ''
    rand = random.choice([0, 1, 2, 3, 4, 5, 6])
    i = 0
    while True:

        if i == 0:
            line += "\t{\t" + random_data(raw_msg) + "\t},"
        else:
            line += "{\t" + random_data(raw_msg) + "\t},"
        i += 1
        if i > rand:
            break

    return line[:-1]


def rand_log():
    data_msg = copy.deepcopy(raw_msg)
    data_log = copy.deepcopy(raw_log)

    line = rand_msg(data_msg)
    CHANGE_OWNER = ''.join(random.choices('0123456789', k=12))
    CHANGE_LOG = ''.join(random.choices('0123456789', k=12))
    data_log = data_log.replace("CHANGE_OWNER", CHANGE_OWNER)
    data_log = data_log.replace("CHANGE_LOG", CHANGE_LOG)
    data_log = data_log.replace("line", line)
    data_log = data_log.replace("  ", "")
    data_log = data_log.replace("\t", "")
    data_log = data_log.replace("\n", "")

    return data_log

FILE_PATH="fake_data/s3/"

def generate_and_write_files(thread_id, num_files):
    print(f"Thread-{thread_id} started.")
    for l in range(num_files):
        logs = ''
        for i in range(30):
            logs += rand_log()

        p_dia = str(random.randint(1, 31)).rjust(2, "0")
        p_ano = "2023"
        p_mes = "06"
        p_hora = str(random.randint(0, 23)).rjust(2, "0")
        FILE_NAME = str(random.getrandbits(128)) + '.json'
        path_patc = f'{FILE_PATH}{p_ano}/{p_mes}/{p_dia}/{p_hora}/'
        os.makedirs(path_patc, exist_ok=True)
        json_file_name = path_patc + '/' + FILE_NAME

        with open(json_file_name, '+a') as file:
            file.write(logs)
        print(f"Thread-{thread_id}: File #{l} stored successfully.")

    print(f"Thread-{thread_id} completed.")


# Number of threads to run
num_threads = 10  # Adjust as needed
num_files_per_thread = 1000  # Number of files to generate per thread

# Create and start the threads
threads = []
for thread_id in range(num_threads):
    thread = threading.Thread(target=generate_and_write_files, args=(thread_id, num_files_per_thread))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All threads completed.")
