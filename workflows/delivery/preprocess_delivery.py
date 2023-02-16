import prefect
from prefect import task
from prefect import Flow
from prefect import Parameter
from prefect import Client
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import S3

import os
import ast
import pathlib
import ftplib
import paramiko
from uuid import uuid4
from zipfile import ZipFile
from dotenv import load_dotenv
import shutil
import yaml
import boto3
from botocore.client import Config


@task(name="collect_delivery_data")
def collect_delivery_data(upload_method, file_structure, ftp_server_url, ftp_server_port, ftp_server_username, ftp_server_password, ftp_server_is_sftp_capable, ftp_path, delivery_upload_process_id, delivery_id, storage_padding_path):
    delivery_padding_directory = "{}/{}/{}".format(storage_padding_path, delivery_upload_process_id, str(uuid4()))
    if not os.path.isdir(delivery_padding_directory):
        os.makedirs(delivery_padding_directory)

    file_structure = ast.literal_eval(file_structure)

    if upload_method == "browser":
        for file_item in file_structure["files"]:
            file_path = file_item["file_path"]
            original_file_name = file_item["original_file_name"]

            target_file_path = "{}/{}".format(delivery_padding_directory, original_file_name)
            shutil.copyfile(file_path, target_file_path)

            if file_item["is_zip_file"]:
                if file_item["extract_zip_file"]:
                    with ZipFile(target_file_path) as zipObj:
                        zipObj.extractall(delivery_padding_directory)
                    os.remove(target_file_path)
    elif upload_method == "ftp":
        ftp_full_path = pathlib.Path(ftp_path)
        ftp_file_name = str(ftp_full_path.name)
        ftp_file_path = str(ftp_full_path.parent)

        # Abprüfen, ob relativer FTP-Pfad geliefert. Falls nicht, via Pathlib das Protokoll und den Host entfernen
        ftp_file_path_parts = ftp_full_path.parent.parts
        if ftp_file_path_parts[0] in tuple(["ftp:", "sftp:"]):
            ftp_file_path_parts = ftp_file_path_parts[-2:]
            ftp_file_path = "/{}".format("/".join(ftp_file_path_parts))

        if ftp_server_is_sftp_capable:
            transport = paramiko.Transport((ftp_server_url, ftp_server_port))
            transport.connect(username=ftp_server_username, password=ftp_server_password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.get("{}/{}".format(ftp_file_path, ftp_file_name), "{}/{}".format(delivery_padding_directory, ftp_file_name))

            sftp.close()
            transport.close()
        else:
            ftp = ftplib.FTP()
            ftp.connect(ftp_server_url, ftp_server_port)
            ftp.login(user=ftp_server_username, passwd=ftp_server_password)
            ftp.cwd(ftp_file_path)

            with open("{}/{}".format(delivery_padding_directory, ftp_file_name), "wb") as output_file:
                ftp.retrbinary('RETR ' + ftp_file_name, output_file.write, 1024)
            ftp.close()

    delivery_file_name = "{}".format(delivery_id)
    if upload_method == "browser":
        shutil.make_archive(os.path.join(delivery_padding_directory, delivery_file_name), "zip", root_dir=delivery_padding_directory)
    else:
        os.rename("{}/{}".format(delivery_padding_directory, ftp_file_name), "{}/{}.zip".format(delivery_padding_directory, delivery_file_name))

    delivery_file_path = "{}/{}.zip".format(delivery_padding_directory, delivery_file_name)
    delivery_data = {}
    delivery_data["delivery_file_name"] = "{}.zip".format(delivery_file_name)
    delivery_data["delivery_file_path"] = delivery_file_path
    return delivery_data


@task(name="store_delivery_data_in_s3")
def store_delivery_data_in_s3(delivery_data, provider_id, delivery_id):
    s3 = boto3.resource('s3',
                        endpoint_url=os.getenv('MINIO_ENDPOINT_URL'),
                        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
                        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    s3_target_file_path = "provider/{}/delivery/{}/{}".format(provider_id, delivery_id, delivery_data["delivery_file_name"])
    s3.Bucket("dpt-delivery-data").upload_file(delivery_data["delivery_file_path"], s3_target_file_path)

    upload_successful = True
    return upload_successful


with Flow(name="Preprocess Delivery", executor=LocalDaskExecutor()) as flow:
    delivery_upload_process_id = Parameter("delivery_upload_process_id")
    provider_id = Parameter("provider_id")
    delivery_id = Parameter("delivery_id")
    upload_method = Parameter("upload_method")
    replace_existing_delivery = Parameter("replace_existing_delivery")
    file_structure = Parameter("file_structure")
    ftp_server_url = Parameter("ftp_server_url")
    ftp_server_port = Parameter("ftp_server_port")
    ftp_server_username = Parameter("ftp_server_username")
    ftp_server_password = Parameter("ftp_server_password")
    ftp_server_is_sftp_capable = Parameter("ftp_server_is_sftp_capable")
    ftp_path = Parameter("ftp_path")
    storage_padding_path = Parameter("storage_padding_path")

    delivery_data = collect_delivery_data(upload_method, file_structure, ftp_server_url, ftp_server_port, ftp_server_username, ftp_server_password, ftp_server_is_sftp_capable, ftp_path, delivery_upload_process_id, delivery_id, storage_padding_path)
    store_data_result = store_delivery_data_in_s3(delivery_data, provider_id, delivery_id)


load_dotenv()
flow.storage = S3(bucket="dpt-prefect-flow-storage", key="workflows/delivery/preprocess_delivery.py", stored_as_script=True, client_options={'endpoint_url': os.getenv('MINIO_ENDPOINT_URL')})

job_template_file_path = "config/k8s_job_template_preprocess_delivery.yaml"
if os.path.isfile(job_template_file_path):
    with open(job_template_file_path) as f:
        job_template = yaml.safe_load(f)
else:
    job_template = {'apiVersion': 'batch/v1', 'kind': 'Job', 'spec': {'template': {'spec': {'containers': [{'name': 'flow', 'env': [{'name': 'GITHUB_REPO_USER', 'valueFrom': {'secretKeyRef': {'name': 'github-repo-credentials', 'key': 'GITHUB_REPO_USER'}}}, {'name': 'GITHUB_REPO_TOKEN', 'valueFrom': {'secretKeyRef': {'name': 'github-repo-credentials', 'key': 'GITHUB_REPO_TOKEN'}}}, {'name': 'PREFECT__CONTEXT__SECRETS__GITHUB_STORAGE_ACCESS_TOKEN', 'valueFrom': {'secretKeyRef': {'name': 'github-repo-credentials', 'key': 'PREFECT__CONTEXT__SECRETS__GITHUB_STORAGE_ACCESS_TOKEN'}}}, {'name': 'MINIO_ENDPOINT_URL', 'valueFrom': {'secretKeyRef': {'name': 'minio-credentials', 'key': 'MINIO_ENDPOINT_URL'}}}, {'name': 'MINIO_ACCESS_KEY', 'valueFrom': {'secretKeyRef': {'name': 'minio-credentials', 'key': 'MINIO_ACCESS_KEY'}}}, {'name': 'MINIO_SECRET_KEY', 'valueFrom': {'secretKeyRef': {'name': 'minio-credentials', 'key': 'MINIO_SECRET_KEY'}}}, {'name': 'PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS', 'valueFrom': {'secretKeyRef': {'name': 'minio-credentials', 'key': 'PREFECT__CONTEXT__SECRETS__AWS_CREDENTIALS'}}}]}]}}}}
flow.run_config = KubernetesRun(image="ghcr.io/olivergoetze/dpt-prefect-delivery-runtime:latest", job_template=job_template)


flow.set_reference_tasks([store_data_result])
c = Client()
try:
    c.create_project(project_name="dpt")  # Projekt DPT automatisch anlegen
except prefect.utilities.exceptions.ClientError:
    # # Fehler abfangen, für den Fall, dass das Projekt bereits exisitiert.
    pass

flow.register(project_name="dpt", version_group_id="dpt_preprocess_delivery")