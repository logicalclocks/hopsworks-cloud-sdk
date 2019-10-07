"""
Contains utility functions for operations related to the feature store
"""

import os

from hops import constants, util


def _do_get_latest_training_dataset_version(training_dataset_name, featurestore_metadata):
    """
    Utility method to get the latest version of a particular training dataset

    Args:
        :training_dataset_name: the training dataset to get the latest version of
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the latest version of the training dataset in the feature store
    """
    training_datasets = featurestore_metadata.training_datasets
    matches = list(
        filter(lambda td: td.name == training_dataset_name, training_datasets.values()))
    versions = list(map(lambda td: int(td.version), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        return 0;


def _get_table_name(featuregroup, version):
    """
    Gets the Hive table name of a featuregroup and version

    Args:
        :featuregroup: the featuregroup to get the table name of
        :version: the version of the featuregroup

    Returns:
        The Hive table name of the featuregroup with the specified version
    """
    return featuregroup + "_" + str(version)


def _do_get_latest_featuregroup_version(featuregroup_name, featurestore_metadata):
    """
    Utility method to get the latest version of a particular featuregroup

    Args:
        :featuregroup_name: the featuregroup to get the latest version of
        :featurestore_metadata: metadata of the featurestore

    Returns:
        the latest version of the featuregroup in the feature store
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    matches = list(filter(lambda fg: fg.name == featuregroup_name, featuregroups))
    versions = list(map(lambda fg: int(fg.version), matches))
    if (len(versions) > 0):
        return max(versions)
    else:
        return 0


def _do_get_featuregroups(featurestore_metadata, online):
    """
    Gets a list of all featuregroups in a featurestore

    Args:
        :featurestore_metadata: the metadata of the featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    if online:
        featuregroups = list(filter(lambda fg: fg.is_online(), featuregroups))
    featuregroup_names = list(map(lambda fg: _get_table_name(fg.name, fg.version), featuregroups))
    return featuregroup_names


def _do_get_features_list(featurestore_metadata, online):
    """
    Gets a list of all features in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the features in this featurestore
    """
    featuregroups = featurestore_metadata.featuregroups.values()
    if online:
        featuregroups = list(filter(lambda fg: fg.is_online(), featuregroups))
    features = []
    for fg in featuregroups:
        features.extend(fg.features)
    features = list(map(lambda f: f.name, features))
    return features


def _do_get_featuregroup_features_list(featuregroup, version, featurestore_metadata):
    """
    Gets a list of all names of features in a featuregroup in a featurestore.

    Args:
        :featuregroup: Featuregroup name.
        :version: Version of the featuregroup to use.
        :featurestore_metadata: Metadata of the featurestore.

    Returns:
        A list of names of the features in this featuregroup.
    """
    featuregroup_version = featuregroup + '_' + str(version)
    features = featurestore_metadata.featuregroups[featuregroup_version].features
    return list(map(lambda f: f.name, features))


def _log(x):
    """
    Generic log function (in case logging is changed from stdout later)

    Args:
        :x: the argument to log

    Returns:
        None
    """
    print(x)


def _do_get_project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    project_name = util.project_name()
    featurestore_name = project_name.lower() + constants.FEATURE_STORE.FEATURESTORE_SUFFIX
    return featurestore_name


def _visualization_validation_warning():
    """
    Checks whether the user is trying to do visualization inside a livy session and prints a warning message
    if the user is trying to plot inside the livy session.

    Returns:
        None

    """
    if constants.ENV_VARIABLES.LIVY_VERSION_ENV_VAR in os.environ:
        _log("Visualizations are not supported in Livy Sessions. "
                      "Use %%local and %matplotlib inline to access the "
                      "python kernel from PySpark notebooks")
