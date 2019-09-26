import json
import sys

from hops import util
from hops import constants
from hops.exceptions import RestAPIError
from hops.featurestore_impl.rest import rest_rpc

BASE_API = "/hopsworks-api/api"
RUN_JOB = ("POST", BASE_API +
           "/project/{project_id}/jobs/{job_name}/executions?action=start")
# Get the latest execution
EXECUTION_STATE = ("GET", BASE_API +
                   "/project/{project_id}/jobs/{job_name}/executions?sort_by=appId:desc&limit=1")


def launch_job(job_name):
    """
    Function for launching a job to Hopsworks. 

    :param job_name: Name of the job to be launched in Hopsworks
    :type job_name: str
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method, endpoint = RUN_JOB
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return rest_rpc._http(endpoint, headers=headers, method=method)


def get_last_execution_info(job_name):
    """
    Function to get information about the last execution

    :param job_name: Name of the job in Hopsworks
    :type job_name: str
    """
    method, endpoint = EXECUTION_STATE
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return rest_rpc._http(endpoint, method=method)
