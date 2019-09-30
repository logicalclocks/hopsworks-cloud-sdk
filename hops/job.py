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
    Launch a job in Hopsworks given the job name - the job needs to exists in Hopsworks before launching it.

    Args:
        job_name (str): Name of the job to launch 

    Returns:
        (str): Json containing Hopsworks response
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method, endpoint = RUN_JOB
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return rest_rpc._http(endpoint, headers=headers, method=method)


def get_last_execution_info(job_name):
    """
    Get information related to the last execution of `job_name`

    Args:
        job_name (str): Name of the Hopsworks job to retrieve the last execution 

    Returns:
        str: Json object containing the information related to the last execution
    """
    method, endpoint = EXECUTION_STATE
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return rest_rpc._http(endpoint, method=method)
