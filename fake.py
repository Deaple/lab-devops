from faker import Faker
import random
import datetime
import uuid
import json
import os

# Initialize Faker
fake = Faker()

def generate_fake_data():
    data = {
        "data": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).isoformat() + "Z",
        "correlationId": str(uuid.uuid4()),
        "timestamp": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).replace(microsecond=0).isoformat() + "-03:00",
        "request": {
            "status": 200,
            "clientSSID": str(uuid.uuid4()),
            "connection": {
                "serial": random.randint(100000, 999999),
                "request_count": 1,
                "pipelined": False,
                "ssl": {
                    "protocol": "TLSv1.2",
                    "cipher": fake.random_element(elements=("ECDHE-RSA-AES256-GCM-SHA384", "AES256-SHA256")),
                    "session_id": str(uuid.uuid4()),
                    "session_reused": False,
                    "client_cert": {
                        "status": "SUCCESS",
                        "serial": fake.random_int(min=100000, max=999999),
                        "fingerprint": fake.uuid4(),
                        "subject": fake.company(),
                        "issuer": fake.company(),
                        "start": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expires": fake.date_time_between(start_date="now", end_date="+1y", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expired": random.choice([True, False])
                    }
                },
                "remote_port": random.randint(1000, 9999),
                "serverOrgID": str(uuid.uuid4()),
                "path": fake.uri(),
                "http_version": 1.1,
                "headers": {
                    "user-agent": fake.user_agent(),
                    "backend": fake.domain_name(),
                    "x-enterprise-correlationid": str(uuid.uuid4()),
                    "host": fake.domain_name(),
                    "accept": "*/*",
                    "securityheader": "notxdetect-true",
                    "content-type": "application/json",
                    "x-client": str(uuid.uuid4()),
                    "x-fapi-interfaction-id": str(uuid.uuid4()),
                    "x-forward-for": fake.ipv4()
                },
                "duration": round(random.uniform(0.001, 5.000), 3),
                "contrato": fake.uri(),
                "consent": str(uuid.uuid4()),
                "bytes_received": random.randint(100, 2000),
                "remote_addr": fake.ipv4(),
                "serverASID": str(uuid.uuid4())
            },
            "response": {
                "status": 200,
                "bytes_sent": random.randint(100, 1000),
                "duration": round(random.uniform(0.001, 5.000), 3),
                "response_addr": fake.ipv4(),
                "headers": {
                    "Content-Type": "application/json",
                    "Content-Length": str(random.randint(50, 200)),
                    "x-amzn-RequestId": str(uuid.uuid4()),
                    "x-amz-apigw-id": str(uuid.uuid4()),
                    "X-Amzn-Trace-Id": "Root=" + str(uuid.uuid4())
                }
            },
            "satus": 200,
            "duration": round(random.uniform(0.001, 5.000), 3)
        }
    }
    return data

def generate_and_save_data(size):
    data_list = []

    # Keep generating data until the total size is at least the target size in MB
    for _ in range(size):
        fake_data = generate_fake_data()
        data_json = json.dumps(fake_data, indent=2)

        # Calculate the size of the data in bytes
        data_size_bytes = len(data_json.encode("utf-8"))

        # Append the data to the list
        data_list.append(fake_data.replace('\n',''))
        print(data_json)

        # If the total size reaches or exceeds the target size, stop generating data
        #if data_size_bytes >= target_size_mb * 1024 * 1024:
        #    break

    # Create a directory to save the data files
    os.makedirs("fake_data", exist_ok=True)

    # Save the data to individual JSON files
    for i, data in enumerate(data_list):
        with open(f"fake_data/data_{i}.json", "w") as file:
            json.dump(data, file, indent=2)


if __name__ == "__main__":
    generate_and_save_data(1)
