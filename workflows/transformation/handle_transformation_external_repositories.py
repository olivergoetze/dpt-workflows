import prefect
from prefect import task
from prefect import Flow
from prefect import unmapped
from prefect import Parameter
from prefect import Client
from prefect.utilities.notifications import slack_notifier
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.backend import FlowRunView
from prefect.storage import GitHub

import os
import sys
import collections
import shutil
import ftplib
import importlib.util
from lxml import etree
from uuid import uuid4
from zipfile import ZipFile
from dotenv import load_dotenv
import time
import subprocess
import yaml


@task(name="fetch_transformation_job_data")
def fetch_transformation_job_data(transformation_job_source_path, transformation_job_source_file):
    """Quelldaten für den Transformations-Job herunterladen und in temporärem Verzeichnis bereitstellen."""
    load_dotenv()
    root_path = os.path.abspath(".")
    source_path = None

    with ftplib.FTP(os.getenv("DDB_FTP_SERVER")) as ftp:
        ftp.login(user=os.getenv("DDB_FTP_USER"), passwd=os.getenv("DDB_FTP_PWD"))
        ftp.cwd(transformation_job_source_path)

        ftp_listdir = []
        try:
            ftp_listdir = ftp.nlst()
        except ftplib.error_perm:
            # Exception bei leerem Ordner abfangen.
            pass

        if transformation_job_source_file in ftp_listdir:
            temp_dir_uuid = str(uuid4().hex)
            temp_dir_path = "temp_dir/{}".format(temp_dir_uuid)
            os.makedirs(temp_dir_path)

            with open("{}/{}".format(temp_dir_path, transformation_job_source_file), "wb") as output_file:
                ftp.retrbinary('RETR ' + transformation_job_source_file, output_file.write, 1024)
                ftp.sendcmd("RNFR {}".format(transformation_job_source_file))
                ftp.sendcmd("RNTO {}.processed".format(
                    transformation_job_source_file))  # Endung .processed anfügen, damit hochgeladene Lieferungen nicht mehrfach prozessiert werden.

            source_path = {"source_path": transformation_job_source_path, "temp_dir": temp_dir_path,
                           "root_path": root_path}

    return source_path


@task(name="prepare_working_directory")
def prepare_working_dir(paths):
    """Vorbereiten des Working-Directories"""
    logger = prefect.context.get("logger")
    # transformation_job_source_path = paths["source_path"]

    working_dir_uuid = str(uuid4().hex)
    working_dir_path = "working_dir/{}".format(working_dir_uuid)
    os.makedirs(working_dir_path)

    # __init__.py-Dateien anlegen, damit DPT-Module importiert werden können.
    with open("working_dir/__init__.py".format(working_dir_path), mode="a"):
        pass
    with open("{}/__init__.py".format(working_dir_path), mode="a"):
        pass

    logger.debug("Working-Directory '{}' angelegt.".format(working_dir_path))
    return working_dir_path


@task(name="prepare_data_preparation_tool_instance")
def prepare_dpt_instance(working_dir_path, dpt_source_path):
    """DPT-Source aus ./dpt_core in das Working-Directory kopieren."""
    dpt_instance_path = "{}/ddbmappings".format(working_dir_path)
    dpt_instance_path_abs = os.path.abspath(dpt_instance_path)
    shutil.copytree(dpt_source_path, dpt_instance_path)

    # __init__.py-Datei anlegen, damit DPT-Module importiert werden können.
    with open("{}/__init__.py".format(dpt_instance_path), mode="a"):
        pass

    # os.makedirs("{}/data_input".format(dpt_instance_path))  # data_input-Verzeichnis anlegen.

    sys.path.append(dpt_instance_path_abs)
    module_name = "handle_session_data.py"
    module_path = "{}/gui_session/{}".format(dpt_instance_path, module_name)
    spec = importlib.util.spec_from_file_location("prepare_first_run", module_path)
    preparation_module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(preparation_module)

    preparation_module.prepare_first_run(root_path=dpt_instance_path_abs)

    sys.path.remove(dpt_instance_path_abs)

    return dpt_instance_path


@task(name="update_dpt_instance")
def update_dpt_instance(dpt_instance_path, paths):
    """DPT-Instanz im Working-Directory per 'git pull' aktualisieren."""
    logger = prefect.context.get("logger")
    root_path = paths["root_path"]

    if os.path.isdir(dpt_instance_path):
        os.chdir(dpt_instance_path)
        logger.info(subprocess.run(['git', 'pull'], stdout=subprocess.PIPE).stdout.decode('utf-8'))
        os.chdir(root_path)
    else:
        logger.warning("DPT-Source-Repository '{}' existiert nicht.".format(dpt_instance_path))


@task(name="fetch_provider_script_repository")
def fetch_provider_script_repository(provider_script_repository, dpt_instance_path, dpt_instance_update_result, paths):
    """Providerskripte aus übergebenen Repositories auschecken."""
    load_dotenv()
    logger = prefect.context.get("logger")
    root_path = paths["root_path"]

    logger.info("CWD: {}".format(os.getcwd()))
    logger.info("root_path: {}".format(root_path))
    logger.info("dpt_instance_path: {}".format(dpt_instance_path))
    logger.info("provider_script_repository: {}".format(provider_script_repository))
    logger.info("paths: {}".format(paths))
    time.sleep(10)

    # Wechsel ins working_dir-Verzeichnis
    os.chdir(dpt_instance_path)
    os.chdir("..")

    normalized_repo_name = provider_script_repository.replace("-", "_")
    normalized_repo_path = "provider_script_repositories/{}".format(normalized_repo_name)
    os.makedirs(normalized_repo_path)

    provider_script_repository_url = "https://{}:{}@github.com/{}".format(os.getenv("GITHUB_REPO_USER"), os.getenv("GITHUB_REPO_TOKEN"), provider_script_repository)
    logger.info(subprocess.run(['git', 'clone', provider_script_repository_url, normalized_repo_path], stdout=subprocess.PIPE).stdout.decode('utf-8'))

    os.chdir(root_path)

@task(name="get_transformation_job_data")
def get_transformation_job_data(working_dir_path, dpt_instance_path, dpt_instance_update_result, paths, provider_script_repo_fetch_result):
    """Daten für den Transformationsjob: data_input.zip mit ISIL-Ordner, der Daten inkl. provider.xml enthält.

    Aus Temp-Directory in Working-Directory kopieren entpacken.
    """
    session_data = collections.OrderedDict()
    temp_dir_path = paths["temp_dir"]

    for input_file in os.listdir(temp_dir_path):
        source_file = "{}/{}".format(temp_dir_path, input_file)
        target_dir = "{}/data_input".format(dpt_instance_path)
        target_file = "{}/{}".format(target_dir, input_file)

        shutil.copyfile(source_file, target_file)

        with ZipFile(target_file) as zipObj:
            zipObj.extractall(target_dir)

        # session_data aus provider.xml (/archiv/cloud_session/*) auslesen
        provider_input_path = target_file.split(".zip")[0]
        provider_xml_path = "{}/provider.xml".format(provider_input_path)
        provider_xml_in = etree.parse(provider_xml_path)


        session_data["provider"] = provider_xml_in.find("//cloud_session/provider").text
        session_data["process_binaries"] = provider_xml_in.find("//cloud_session/process_binaries").text
        session_data["enable_mets_generation"] = provider_xml_in.find("//cloud_session/enable_mets_generation").text
        session_data["mets_application_profile"] = provider_xml_in.find("//cloud_session/mets_application_profile").text
        session_data["mets_logo_url"] = provider_xml_in.find("//cloud_session/mets_logo_url").text
        session_data["mets_mail_address"] = provider_xml_in.find("//cloud_session/mets_mail_address").text
        session_data["mets_url_prefix"] = provider_xml_in.find("//cloud_session/mets_url_prefix").text
        session_data["enrich_rights_info"] = provider_xml_in.find("//cloud_session/enrich_rights_info").text
        session_data["enable_ddb2017_preprocessing"] = provider_xml_in.find("//cloud_session/enable_ddb2017_preprocessing").text
        session_data["enrich_aggregator_info"] = provider_xml_in.find("//cloud_session/enrich_aggregator_info").text
        session_data["apply_mapping_definition"] = provider_xml_in.find("//cloud_session/apply_mapping_definition").text

    return session_data


@task(name="handle_transformation_run", log_stdout=True)
def handle_transformation_run(transformation_job, dpt_instance_path):
    """Transformation ausführen."""
    logger = prefect.context.get("logger")
    dpt_instance_path_abs = os.path.abspath(dpt_instance_path)

    logger.debug("CWD: {}".format(os.getcwd()))
    sys.path.append(dpt_instance_path_abs)
    logger.debug("Path vor DPT-Aufruf: {}".format("\n".join(sys.path)))

    module_name = "transformation_p1.py"
    module_path = "{}/{}".format(dpt_instance_path, module_name)
    spec = importlib.util.spec_from_file_location("run_transformation_p1", module_path)
    transformation_module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(transformation_module)

        transformation_module.run_transformation_p1(root_path=dpt_instance_path_abs, session_data=transformation_job, is_gui_session=True, propagate_logging=True, is_unattended_session=True)
    except ModuleNotFoundError as e:
        logger.warning(e)

    sys.path.remove(dpt_instance_path_abs)
    logger.debug("Path nach DPT-Aufruf: {}".format("\n".join(sys.path)))

@task(name="monitor_transformation_run", log_stdout=True)
def monitor_transformation_run(transformation_job, dpt_instance_path, paths):
    logger = prefect.context.get("logger")
    root_path = paths["root_path"]

    processing_status_file = "{}/{}/gui_session/processing_status.xml".format(root_path, dpt_instance_path)  # root_path übergeben, da das CWD durch die parallele Ausführung von handle_transformation_run manipuliert wird und dadurch die processing_status.xml nicht mehr gefunden wird.
    processing_status_input = None

    transformation_run_finished = False
    previous_processing_status = {}
    flow_run_view = FlowRunView.from_flow_run_id(prefect.context.get("flow_run_id"))

    while not transformation_run_finished:
        if os.path.isfile(processing_status_file):
            try:
                processing_status_input = etree.parse(processing_status_file)
            except etree.XMLSyntaxError:
                logger.debug("Aktualisierung der Status-Information übersprungen.")

        if processing_status_input is not None:
            # processing_step, status_message und error_status als Dictionary speichern und dieses als String loggen.
            #   im Frontend kann der String dann über ast.literal_eval() wieder als Dictionary dekodiert werden.
            processing_status = {}
            processing_status["processing_step"] = processing_status_input.find("//processing_step").text
            processing_status["status_message"] = processing_status_input.find("//status_message").text
            processing_status["error_status"] = processing_status_input.find("//error_status").text
            processing_status["workflow_module"] = processing_status_input.find("//workflow_module").text
            processing_status["workflow_module_type"] = processing_status_input.find("//workflow_module_type").text
            processing_status["current_input_file"] = processing_status_input.find("//current_input_file").text
            processing_status["current_input_type"] = processing_status_input.find("//current_input_type").text
            processing_status["input_file_progress"] = processing_status_input.find("//input_file_progress").text
            processing_status["input_file_count"] = processing_status_input.find("//input_file_count").text

            if processing_status != previous_processing_status:
                logger.info(processing_status)
                previous_processing_status = processing_status

        # prüfen, ob Task "handle_transformation_run" oder der übergreifende Flow im Status "Finished" ist. Falls ja, transformation_run_finished = True setzen.
        flow_run_view = flow_run_view.get_latest()
        task_run_view = flow_run_view.get_task_run(task_slug='handle_transformation_run-1')

        if task_run_view.state.is_finished() or flow_run_view.state.is_finished():
            transformation_run_finished = True
        else:
            time.sleep(1)

@task(name="upload_transformation_job_result")
def upload_transformation_job_result(transformation_job_result, paths, dpt_instance_path, transformation_job_data):
    """Output-Ordner in FTP-Verzeichnis hochladen."""
    logger = prefect.context.get("logger")
    upload_successful = True
    root_path = paths["root_path"]
    os.chdir(root_path)
    load_dotenv()

    data_output_path = "{}/data_output".format(dpt_instance_path)
    os.chdir("{}/{}".format(root_path, data_output_path))

    provider_isil = transformation_job_data["provider"]
    transformation_job_result_folder = provider_isil.replace("-", "_")
    transformation_job_result_file = "{}.zip".format(transformation_job_result_folder)

    shutil.make_archive(transformation_job_result_folder, "zip", transformation_job_result_folder)

    transformation_job_source_path = paths["source_path"]
    with ftplib.FTP(os.getenv("DDB_FTP_SERVER")) as ftp:
        ftp.login(user=os.getenv("DDB_FTP_USER"), passwd=os.getenv("DDB_FTP_PWD"))
        ftp.cwd(transformation_job_source_path)

        # Verzeichnisliste abrufen und prüfen, ob Ordner "processed" zur Ablage der Ergebnisse vorhanden ist.
        ftp_listdir = []
        try:
            ftp_listdir = ftp.nlst()
        except ftplib.error_perm:
            # Exception bei leerem Ordner abfangen.
            pass
        if "processed" not in ftp_listdir:
            # Ordner "processed" auf FTP anlegen, wenn noch nicht vorhanden.
            ftp.mkd("processed")

        ftp.cwd("{}/processed".format(transformation_job_source_path))
        try:
            ftp.storbinary("STOR " + transformation_job_result_file, open(transformation_job_result_file, "rb"))
        except ConnectionResetError as exc:
            logger.error("Verbindung durch FTP-Server unterbrochen. Die Output-Daten werden im Ordner '{}' belassen, damit diese manuell heruntergeladen werden können.\n{}".format(data_output_path, exc))
            upload_successful = False

    return upload_successful


@task(name="cleanup_working_directory", trigger=prefect.triggers.always_run)
def cleanup_working_dir(transformation_result_upload, paths, working_dir):
    """Bereinigen des Working-Directories.

    Der Task wird auch ausgeführt, wenn vorherige Tasks fehlschlagen, damit das Working-Directory vor einem neuen Flow-Run immer bereinigt wurde.
    """
    logger = prefect.context.get("logger")
    root_path = paths["root_path"]
    temp_dir = paths["temp_dir"]
    os.chdir(root_path)

    cleanup_paths = [temp_dir]
    if transformation_result_upload:
        cleanup_paths.append(working_dir)
    for cleanup_path in tuple(cleanup_paths):
        if os.path.isdir(cleanup_path):
            shutil.rmtree(cleanup_path)

            logger.debug("Cleanup-Directory '{}' entfernt.".format(cleanup_path))


# with Flow(name="DPT-Transformation Testing", state_handlers=[slack_notifier], executor=LocalDaskExecutor()) as flow:
with Flow(name="DPT-Transformation Testing External Repositories", executor=LocalDaskExecutor()) as flow:
    dpt_source = Parameter("dpt_source", default="dpt_core")
    provider_script_repositories = Parameter("provider_script_repositories", default=["olivergoetze/dpt-provider-scripts", "olivergoetze/dpt-core-test", "olivergoetze/ddbmappings", "olivergoetze/dpt-kubernetes-secrets"])
    transformation_job_source_path = Parameter("transformation_job_source_path", default="/Fachstelle_Archiv/datapreparationcloud")
    transformation_job_source_file = Parameter("transformation_job_source_file", default="DE_1983.zip")

    path_dict = fetch_transformation_job_data(transformation_job_source_path, transformation_job_source_file)
    working_dir = prepare_working_dir(path_dict)
    dpt_instance = prepare_dpt_instance(working_dir, dpt_source)
    dpt_instance_update_result = update_dpt_instance(dpt_instance, path_dict)
    provider_script_repo_fetch_result = fetch_provider_script_repository.map(provider_script_repository=provider_script_repositories, dpt_instance_path=unmapped(dpt_instance), dpt_instance_update_result=unmapped(dpt_instance_update_result), paths=unmapped(path_dict))
    transformation_job_data = get_transformation_job_data(working_dir, dpt_instance, dpt_instance_update_result, path_dict, provider_script_repo_fetch_result)
    transformation_result = handle_transformation_run(transformation_job_data, dpt_instance)
    monitoring_result = monitor_transformation_run(transformation_job_data, dpt_instance, path_dict)
    transformation_result_upload = upload_transformation_job_result(transformation_result, path_dict, dpt_instance, transformation_job_data)

    cleanup_dir = cleanup_working_dir(transformation_result_upload, path_dict, working_dir)


flow.storage = GitHub(repo="olivergoetze/dpt-workflows", path="workflows/transformation/handle_transformation_external_repositories.py")

job_template_file_path = "config/k8s_job_template_handle_transformation_external_repositories.yaml"
if os.path.isfile(job_template_file_path):
    with open(job_template_file_path) as f:
        job_template = yaml.safe_load(f)
else:
    job_template = {'apiVersion': 'batch/v1', 'kind': 'Job', 'spec': {'template': {'spec': {'containers': [{'name': 'flow', 'env': [{'name': 'DDB_FTP_SERVER', 'valueFrom': {'secretKeyRef': {'name': 'ddbftp-credentials', 'key': 'DDB_FTP_SERVER'}}}, {'name': 'DDB_FTP_USER', 'valueFrom': {'secretKeyRef': {'name': 'ddbftp-credentials', 'key': 'DDB_FTP_USER'}}}, {'name': 'DDB_FTP_PWD', 'valueFrom': {'secretKeyRef': {'name': 'ddbftp-credentials', 'key': 'DDB_FTP_PWD'}}}, {'name': 'GITHUB_REPO_USER', 'valueFrom': {'secretKeyRef': {'name': 'github-repo-credentials', 'key': 'GITHUB_REPO_USER'}}}, {'name': 'GITHUB_REPO_TOKEN', 'valueFrom': {'secretKeyRef': {'name': 'github-repo-credentials', 'key': 'GITHUB_REPO_TOKEN'}}}]}]}}}}
flow.run_config = KubernetesRun(image="ghcr.io/olivergoetze/dpt-core-test:latest", job_template=job_template)

flow.set_reference_tasks([transformation_result_upload])
c = Client()
try:
    c.create_project(project_name="dpt")  # Projekt "dpt" automatisch anlegen
except prefect.utilities.exceptions.ClientError:
    # Fehler abfangen, für den Fall, dass das Projekt bereits exisitiert.
    pass
flow.register(project_name="dpt", version_group_id="dpt_testing_external_repositories")
