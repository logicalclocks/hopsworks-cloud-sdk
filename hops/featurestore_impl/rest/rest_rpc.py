"""
REST calls to Hopsworks Feature Store Service
"""

import os

from hops import constants, util
from hops.exceptions import RestAPIError


def _http(resource_url, headers=None, method=constants.HTTP_CONFIG.HTTP_GET, data=None):
    response = util.send_request(
        method, resource_url, headers=headers, data=data)
    response_object = response.json()

    if (response.status_code // 100) != 2:
        error_code, error_msg, user_msg = util._parse_rest_error(
            response_object)
        raise RestAPIError("Could not execute HTTP request (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                               resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    return response_object


def _get_api_path():
    return constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE \
        + constants.DELIMITERS.SLASH_DELIMITER + constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE \
        + constants.DELIMITERS.SLASH_DELIMITER


def _get_api_project_path():
    return _get_api_path() + util.project_id()


def _get_api_featurestore_path():
    return _get_api_project_path() + constants.DELIMITERS.SLASH_DELIMITER \
        + constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE


def _get_api_featurestore_path_name(featurestore):
    return _get_api_featurestore_path() + constants.DELIMITERS.SLASH_DELIMITER + featurestore


def _get_api_featurestore_path_id(featurestore_id):
    return _get_api_featurestore_path() + constants.DELIMITERS.SLASH_DELIMITER + str(featurestore_id)


def _get_featurestores():
    """
    Sends a REST request to get all featurestores for the project

    Returns:
        a list of Featurestore JSON DTOs

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(_get_api_featurestore_path())


def _get_featurestore_metadata(featurestore):
    """
    Makes a REST call to hopsworks to get all metadata of a featurestore (featuregroups and
    training datasets) for the provided featurestore.

    Args:
        :featurestore: the name of the database, defaults to the project's featurestore

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(_get_api_featurestore_path_name(featurestore) + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_METADATA_RESOURCE)


def _get_project_info(project_name):
    """
    Makes a REST call to hopsworks to get all metadata of a project for the provided project.

    Args:
        :project_name: the name of the project

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_INFO_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 project_name)


def _get_credentials(project_id):
    """
    Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

    Args:
        :project_name: id of the project

    Returns:
        JSON response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return _http(constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 project_id + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_CREDENTIALS_RESOURCE)


def _get_featuregroup_rest(featuregroup_id, featurestore_id):
    """
    Makes a REST call to hopsworks for getting the metadata of a particular featuregroup (including the statistics)

    Args:
        :featuregroup_id: id of the featuregroup
        :featurestore_id: id of the featurestore where the featuregroup resides

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    return _http(_get_api_featurestore_path_id(featurestore_id) +
                 constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_FEATUREGROUPS_RESOURCE +
                 constants.DELIMITERS.SLASH_DELIMITER + str(featuregroup_id))


def _get_training_dataset_rest(training_dataset_id, featurestore_id):
    """
    Makes a REST call to hopsworks for getting the metadata of a particular training dataset (including the statistics)

    Args:
        :training_dataset_id: id of the training_dataset
        :featurestore_id: id of the featurestore where the training dataset resides

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    return _http(_get_api_featurestore_path_id(featurestore_id) +
                 constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_RESOURCE +
                 constants.DELIMITERS.SLASH_DELIMITER
                 + str(training_dataset_id), headers=headers)


def _put_featuregroup_import_job(job_conf):
    """
    Makes a REST call to hopsworks to configure a featuregroup import job 

    Args:
        :job_conf: featuregroup import job configuration 

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (_get_api_featurestore_path() +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_FEATUREGROUP_IMPORT_RESOURCE)
    return _http(resource_url, headers=headers, method=constants.HTTP_CONFIG.HTTP_PUT, data=job_conf)


def _get_online_featurestore_jdbc_connector_rest(featurestore_id):
    """
    Makes a REST call to Hopsworks to get the JDBC connection to the online feature store
    Args:
        :featurestore_id: the id of the featurestore
    Returns:
        the http response
    """
    return _http(constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                 str(featurestore_id) + constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_FEATURESTORES_STORAGE_CONNECTORS_RESOURCE +
                 constants.DELIMITERS.SLASH_DELIMITER +
                 constants.REST_CONFIG.HOPSWORKS_ONLINE_FEATURESTORE_STORAGE_CONNECTOR_RESOURCE)


def _put_trainingdataset_create_job(job_conf):
    """
    Makes a REST call to hopsworks to configure a training dataset creation job

    Args:
        :job_conf: training dataset creation job configuration

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    resource_url = (_get_api_featurestore_path() +
                    constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_TRAININGDATASETS_CREATION_RESOURCE)
    return _http(resource_url, method = constants.HTTP_CONFIG.HTTP_POST, headers=headers, data=job_conf)
