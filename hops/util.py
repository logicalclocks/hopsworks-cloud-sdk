"""

Miscellaneous utility functions for user applications.

"""

import base64
import json
import os
from socket import socket
from urllib.parse import urlparse

import boto3
import idna
from OpenSSL import SSL
from cryptography import x509
from cryptography.x509.oid import NameOID
from pyhive import hive

from hops import constants
from hops.exceptions import UnkownSecretStorageError

try:
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

verify = None
session = None


def project_id():
    """
    Get the Hopsworks project id from environment variables

    Returns: the Hopsworks project id

    """
    return os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR]


def project_name():
    """
    Extracts the project name from the environment

    Returns:
        project name
    """
    return os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR]


def _get_hopsworks_rest_endpoint():
    """

    Returns:
        The hopsworks REST endpoint for making requests to the REST API

    """
    return 'https://' + os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]


hopsworks_endpoint = None
try:
    hopsworks_endpoint = _get_hopsworks_rest_endpoint()
except:
    pass


def _get_host_port_pair():
    """
    Removes "http or https" from the rest endpoint and returns a list
    [endpoint, port], where endpoint is on the format /path.. without http://

    Returns:
        a list [endpoint, port]
    """
    endpoint = _get_hopsworks_rest_endpoint()
    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index + 1:]
    host_port_pair = endpoint.split(':')
    return host_port_pair


def set_auth_header(headers):
    headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "ApiKey " + \
        os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR]


def get_requests_verify(hostname_verification=True, trust_store_path=None):
    """
    Get verification method for sending HTTP requests to Hopsworks.
    Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c
    Returns:
        if env var HOPS_UTIL_VERIFY is not false
            then if hopsworks certificate is self-signed, return the path to the truststore (PEM)
            else if hopsworks is not self-signed, return true
        return false
    """
    if hostname_verification:
        hostname, port = _get_host_port_pair()
        hostname_idna = idna.encode(hostname)
        sock = socket()

        sock.connect((hostname, int(port)))
        ctx = SSL.Context(SSL.SSLv23_METHOD)
        ctx.check_hostname = False
        ctx.verify_mode = SSL.VERIFY_NONE

        sock_ssl = SSL.Connection(ctx, sock)
        sock_ssl.set_connect_state()
        sock_ssl.set_tlsext_host_name(hostname_idna)
        sock_ssl.do_handshake()
        cert = sock_ssl.get_peer_certificate()
        crypto_cert = cert.to_cryptography()
        sock_ssl.close()
        sock.close()

        try:
            commonname = crypto_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[
                0].value
            issuer = crypto_cert.issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[
                0].value
            if commonname == issuer and trust_store_path:
                return trust_store_path
            else:
                return True
        except x509.ExtensionNotFound:
            return True

    return False


def prepare_requests(hostname_verification=True, trust_store_path=None):
    global verify
    global session
    session = requests.session()
    verify = get_requests_verify(hostname_verification=hostname_verification,
                                 trust_store_path=trust_store_path)

def send_request(method, resource, data=None, headers=None):
    """
    Sends a request to Hopsworks. In case of Unauthorized response, submit the request once more as jwt might not
    have been read properly from local container.

    Args:
        method: HTTP(S) method
        resource: Hopsworks resource
        data: HTTP(S) payload
        headers: HTTP(S) headers
        verify: Whether to verify the https request

    Returns:
        HTTP(S) response
    """
    if headers is None:
        headers = {}

    set_auth_header(headers)
    url = _get_hopsworks_rest_endpoint() + resource
    req = requests.Request(method, url, data=data, headers=headers)
    prepped = session.prepare_request(req)
    response = session.send(prepped, verify=verify)

    if response.status_code == constants.HTTP_CONFIG.HTTP_UNAUTHORIZED:
        set_auth_header(headers)
        prepped = session.prepare_request(req)
        response = session.send(prepped)
    return response


def _create_hive_connection(featurestore):
    """Returns Hive connection

    Args:
        :featurestore: featurestore to which connection will be established
    """
    host = urlparse(_get_hopsworks_rest_endpoint()).hostname
    hive_conn = hive.Connection(host=host,
                                port=9085,
                                database=featurestore,
                                auth='CERTIFICATES',
                                truststore='trustStore.jks',
                                keystore='keyStore.jks',
                                keystore_password=os.environ[constants.ENV_VARIABLES.CERT_KEY_ENV_VAR])

    return hive_conn


def _parse_rest_error(response_dict):
    """
    Parses a JSON response from hopsworks after an unsuccessful request

    Args:
        response_dict: the JSON response represented as a dict

    Returns:
        error_code, error_msg, user_msg
    """
    error_code = -1
    error_msg = ""
    user_msg = ""
    if constants.REST_CONFIG.JSON_ERROR_CODE in response_dict:
        error_code = response_dict[constants.REST_CONFIG.JSON_ERROR_CODE]
    if constants.REST_CONFIG.JSON_ERROR_MSG in response_dict:
        error_msg = response_dict[constants.REST_CONFIG.JSON_ERROR_MSG]
    if constants.REST_CONFIG.JSON_USR_MSG in response_dict:
        user_msg = response_dict[constants.REST_CONFIG.JSON_USR_MSG]
    return error_code, error_msg, user_msg


def get_secret(secrets_store, secret_key):
    """
    Returns secret value from the AWS Secrets Manager or Parameter Store

    Args:
        :secrets_store: the underlying secrets storage to be used, e.g. `secretsmanager` or `parameterstore`
        :secret_type (str): key for the secret value, e.g. `api-key`, `cert-key`, `trust-store`, `key-store`
    Returns:
        :str: secret value
    """
    if secrets_store == constants.AWS.SECRETS_MANAGER:
        return _query_secrets_manager(secret_key)
    elif secrets_store == constants.AWS.PARAMETER_STORE:
        return _query_parameter_store(secret_key)
    else:
        raise UnkownSecretStorageError(
            "Secrets storage " + secrets_store + " is not supported.")


def _assumed_role():
    client = boto3.client('sts')
    response = client.get_caller_identity()
    # arns for assumed roles in SageMaker follow the following schema
    # arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name
    local_identifier = response['Arn'].split(':')[-1].split('/')
    if len(local_identifier) != 3 or local_identifier[0] != 'assumed-role':
        raise Exception(
            'Failed to extract assumed role from arn: ' + response['Arn'])
    return local_identifier[1]


def _query_secrets_manager(secret_key):
    secret_name = 'hopsworks/role/' + _assumed_role()

    session = boto3.session.Session()
    if (os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] != constants.AWS.DEFAULT_REGION):
        region_name = os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR]
    else:
        region_name = session.region_name

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])[secret_key]


def _query_parameter_store(secret_key):
    ssm = boto3.client('ssm')
    name = '/hopsworks/role/' + _assumed_role() + '/type/' + secret_key
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']


def write_b64_cert_to_bytes(b64_string, path):
    """Converts b64 encoded certificate to bytes file .

    Args:
        :b64_string (str): b64 encoded string of certificate
        :path (str): path where file is saved, including file name. e.g. /path/key-store.jks
    """

    with open(path, 'wb') as f:
        cert_b64 = base64.b64decode(b64_string)
        f.write(cert_b64)


def abspath(hdfs_path):
    return hdfs_path
