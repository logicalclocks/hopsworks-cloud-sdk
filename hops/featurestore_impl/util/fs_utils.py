"""
Contains utility functions for operations related to the feature store
"""

import os
import re

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


def _do_get_training_dataset_features_list(training_dataset, version, featurestore_metadata):
    """
    Gets a list of all names of features in a training dataset in a featurestore.

    Args:
        :training_dataset: Training dataset name.
        :version: Version of the training dataset to use.
        :featurestore_metadata: Metadata of the featurestore.

    Returns:
        A list of names of the features in this training dataset.
    """
    training_dataset_version = training_dataset + '_' + str(version)
    features = featurestore_metadata.training_datasets[training_dataset_version].features
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


def _validate_metadata(name, description, featurestore_settings):
    """
    Function for validating metadata when creating new feature groups and training datasets.
    Raises and assertion exception if there is some error in the metadata.

    Args:
        :name: the name of the feature group/training dataset
        :description: the description
        :featurestore_regex: Regex string to match featuregroup/training dataset and feature names with

    Returns:
        None

    Raises:
        :ValueError: if the metadata does not match the required validation rules
    """
    if not featurestore_settings.featurestore_regex.match(name):
        raise ValueError("Illegal feature store entity name, the provided name {} is invalid. Entity names can only "
            "contain lower case characters, numbers and underscores and cannot be longer than {} characters or empty."
            .format(name, featurestore_settings.entity_name_max_len))

    if description:
        if len(description) > featurestore_settings.entity_description_max_len:
            raise ValueError("Illegal feature store entity description, the provided description for the entity {} is "
                "too long with {} characters. Entity descriptions cannot be longer than {} characters."
                .format(name, len(description), featurestore_settings.entity_description_max_len))