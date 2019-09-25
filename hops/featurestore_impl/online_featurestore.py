"""
Online Feature Store functions
"""
from hops import constants
from hops.featurestore_impl.exceptions.exceptions import OnlineFeaturestorePasswordOrUserNotFound

def _get_online_feature_store_password_and_user(storage_connector):
    """
    Extracts the password and user from an online featurestore storage connector
    Args:
        :storage_connector: the storage connector of the online feature store
    Returns:
        The password and username for the online feature store of the storage connector
    Raises:
        :OnlineFeaturestorePasswordOrUserNotFound: if a password or user could not be found
    """
    args = storage_connector.arguments.split(constants.DELIMITERS.COMMA_DELIMITER)
    pw = ""
    user = ""
    for arg in args:
        if "password=" in arg:
            pw = arg.replace("password=", "")
        if "user=" in arg:
            user = arg.replace("user=", "")
    if pw == "" or user =="":
        raise OnlineFeaturestorePasswordOrUserNotFound("A password/user for the online feature store was not found")
    return pw, user