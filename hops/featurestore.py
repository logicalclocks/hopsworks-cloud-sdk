"""
A feature store client. This module exposes an API for interacting with feature stores in Hopsworks.
It hides complexity and provides utility methods such as:

    - `connect()`.
    - `project_featurestore()`.
    - `get_featuregroup()`.
    - `get_feature()`.
    - `get_features()`.
    - `sql()`
    - `get_featurestore_metadata()`
    - `get_project_featurestores()`
    - `get_featuregroups()`
    - `get_training_datasets()`

Below is some example usages of this API (assuming you have two featuregroups called
'trx_graph_summary_features' and 'trx_summary_features' with schemas:

 |-- cust_id: integer (nullable = true)

 |-- pagerank: float (nullable = true)

 |-- triangle_count: float (nullable = true)

and

 |-- cust_id: integer (nullable = true)

 |-- min_trx: float (nullable = true)

 |-- max_trx: float (nullable = true)

 |-- avg_trx: float (nullable = true)

 |-- count_trx: long (nullable = true)

, respectively.

    >>> from hops import featurestore
    >>>
    >>> # Connect to a feature store
    >>> featurestore.connect('my_hopsworks_hostname', 'my_project')
    >>>
    >>> # Get feature group example
    >>> #The API will default to version 1 for the feature group and the project's own feature store
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features")
    >>> #You can also explicitly define version and feature store:
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      featuregroup_version = 1)
    >>>
    >>> # Get single feature example
    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's
    >>> # own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx",
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            featuregroup="trx_summary_features",
    >>>                                            featuregroup_version = 1)
    >>> # When you want to get features from different feature groups the API will infer how to join the features
    >>> # together
    >>>
    >>> # Get list of features example
    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore(),
    >>>                                      featuregroups_version_dict={"trx_graph_summary_features": 1,
    >>>                                                                  "trx_summary_features": 1},
    >>>                                                                  join_key="cust_id")
    >>>
    >>> # Run SQL query against feature store example
    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5",
    >>>                  featurestore=featurestore.project_featurestore()).show(5)
    >>>
    >>> # Get featurestore metadata example
    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be
    >>> # specified with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())
    >>>
    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be
    >>> # specified with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())
    >>>
    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()
    >>> # By default `get_featurestore_metadata` will use the project's feature store, but this can also be
    >>> # specified with the optional argument featurestore
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())
    >>>
    >>> # After a managed dataset have been created, it is easy to share it and re-use it for training various models.
    >>> # For example if the dataset have been materialized in tf-records format you can call the method
    >>> # get_training_dataset_path(training_dataset)
    >>> # to get the HDFS path and read it directly in your tensorflow code.
    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1,
    >>> # but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(),
    >>> training_dataset_version=1)
"""

import os
import json

from hops import util, constants, job
from hops.featurestore_impl import core
from hops.featurestore_impl.exceptions.exceptions import FeatureVisualizationError
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils


update_cache_default = True


def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    return fs_utils._do_get_project_featurestore()


def get_featuregroup(featuregroup, featurestore=None, featuregroup_version=1, online=False):
    """
    Gets a featuregroup from a featurestore as a pandas dataframe

    Example usage:

    >>> #The API will default to version 1 for the feature group and the project's own feature store
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features")
    >>> #You can also explicitly define version and feature store:
    >>> trx_summary_features = featurestore.get_featuregroup("trx_summary_features",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      featuregroup_version = 1)

    Args:
        :featuregroup: the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        a dataframe with the contents of the featuregroup

    """
    if featurestore is None:
        featurestore = project_featurestore()

    try:  # Try with cached metadata
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore,
                                                                         update_cache=update_cache_default),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         online=online)
    except:  # Try again after updating the cache
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore, update_cache=True),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         online=online)


def get_feature(feature, featurestore=None, featuregroup=None, featuregroup_version=1, online=False):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries
    hopsworks metastore to see if the feature exists in any of the featuregroups in the featurestore.
    If the user knows which featuregroup contain the feature, it should be specified as it will improve
    performance of the query. Will first try to construct the query from the cached metadata, if that fails,
    it retries after updating the cache

    Example usage:

    >>> #The API will infer the featuregroup and default to version 1 for the feature group with this and the project's
    >>> # own feature store
    >>> max_trx_feature = featurestore.get_feature("max_trx")
    >>> #You can also explicitly define feature group,version and feature store:
    >>> max_trx_feature = featurestore.get_feature("max_trx", featurestore=featurestore.project_featurestore(),
    >>> featuregroup="trx_summary_features", featuregroup_version = 1)

    Args:
        :feature: the feature name to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup: (Optional) the featuregroup where the feature resides
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        A dataframe with the feature

    """
    try:  # try with cached metadata
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore,
                                                                             update_cache=update_cache_default),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, online=online)
    except:  # Try again after updating cache
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=True),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, online=online)


def get_features(features, featurestore=None, featuregroups_version_dict={}, join_key=None, online=False):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored. It will try to construct the query first from the cached metadata,
    if that fails it will re-try after reloading the cache

    Example usage:

    >>> # The API will default to version 1 for feature groups and the project's feature store
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                      featurestore=featurestore.project_featurestore())
    >>> #You can also explicitly define feature group, version, feature store, and join-key:
    >>> features = featurestore.get_features(["pagerank", "triangle_count", "avg_trx"],
    >>>                                     featurestore=featurestore.project_featurestore(),
    >>>                                     featuregroups_version_dict={"trx_graph_summary_features": 1,
    >>>                                     "trx_summary_features": 1}, join_key="cust_id")

    Args:
        :features: a list of features to get from the featurestore
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :join_key: (Optional) column name to join on
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        A dataframe with all the features

    """
    # try with cached metadata
    try:
        return core._do_get_features(features,
                                     core._get_featurestore_metadata(featurestore,
                                                                     update_cache=update_cache_default),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key,
                                     online=online)
        # Try again after updating cache
    except:
        return core._do_get_features(features, core._get_featurestore_metadata(featurestore, update_cache=True),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key,
                                     online=online)


def sql(query, featurestore=None, online=False):
    """
    Executes a generic SQL query on the featurestore via pyHive

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5").show(5)
    >>> # You can also explicitly define the feature store
    >>> featurestore.sql("SELECT * FROM trx_graph_summary_features_1 WHERE triangle_count > 5",
    >>>                  featurestore=featurestore.project_featurestore()).show(5)

    Args:
        :query: SQL query
        :featurestore: the featurestore to query, defaults to the project's featurestore
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        (pandas.DataFrame): A pandas dataframe with the query results
    """
    if featurestore is None:
        featurestore = project_featurestore()

    dataframe = core._run_and_log_sql(query, featurestore, online)

    return dataframe


def get_featurestore_metadata(featurestore=None, update_cache=False):
    """
    Sends a REST call to Hopsworks to get the list of featuregroups and their features for the given featurestore.

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.get_featurestore_metadata()
    >>> # You can also explicitly define the feature store
    >>> featurestore.get_featurestore_metadata(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to query metadata of
        :update_cache: if true the cache is updated
        :online: a boolean flag whether to fetch the online feature group or the offline one (assuming that the
                 feature group has online serving enabled)

    Returns:
        A list of featuregroups and their metadata

    """
    if featurestore is None:
        featurestore = project_featurestore()
    return core._get_featurestore_metadata(featurestore=featurestore, update_cache=update_cache)


def get_featuregroups(featurestore=None, online=False):
    """
    Gets a list of all featuregroups in a featurestore, uses the cached metadata.

    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list featuregroups for, defaults to the project-featurestore
        :online: flag whether to filter the featuregroups that have online serving enabled

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()

    # Try with the cache first
    try:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore,
                                                                              update_cache=update_cache_default),
                                              online=online)
    # If it fails, update cache
    except:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=True),
                                                                              online=online)


def get_features_list(featurestore=None, online=False):
    """
    Gets a list of all features in a featurestore, will use the cached featurestore metadata

    >>> # List all Features in a Feature Store
    >>> featurestore.get_features_list()
    >>> # By default `get_features_list()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_features_list(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list features for, defaults to the project-featurestore
        :online: flag whether to filter the features that have online serving enabled

    Returns:
        A list of names of the features in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore,
                                                                              update_cache=update_cache_default,),
                                              online=online)
    except:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=True,),
                                              online=online)


def get_featuregroup_features_list(featuregroup, version=None, featurestore=None):
    """
    Gets a list of the names of the features in a featuregroup.

    Args:
        :featuregroup: Name of the featuregroup to get feature names for.
        :version: Version of the featuregroup to use. Defaults to the latest version.
        :featurestore: The featurestore to list features for. Defaults to project-featurestore.

    Returns:
        A list of names of the features in this featuregroup.
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        if version is None:
            version = fs_utils._do_get_latest_featuregroup_version(
                featuregroup, core._get_featurestore_metadata(featurestore, update_cache=False))
        return fs_utils._do_get_featuregroup_features_list(
            featuregroup, version, core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        if version is None:
            version = fs_utils._do_get_latest_featuregroup_version(
                featuregroup, core._get_featurestore_metadata(featurestore, update_cache=True))
        return fs_utils._do_get_featuregroup_features_list(
            featuregroup, version, core._get_featurestore_metadata(featurestore, update_cache=True))


def get_training_dataset_features_list(training_dataset, version=None, featurestore=None):
    """
    Gets a list of the names of the features in a training dataset.

    Args:
        :training_dataset: Name of the training dataset to get feature names for.
        :version: Version of the training dataset to use. Defaults to the latest version.
        :featurestore: The featurestore to look for the dataset for. Defaults to project-featurestore.

    Returns:
        A list of names of the features in this training dataset.
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        if version is None:
            version = fs_utils._do_get_latest_training_dataset_version(
                training_dataset, core._get_featurestore_metadata(featurestore, update_cache=False))
        return fs_utils._do_get_training_dataset_features_list(
            training_dataset, version, core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        if version is None:
            version = fs_utils._do_get_latest_training_dataset_version(
                training_dataset, core._get_featurestore_metadata(featurestore, update_cache=True))
        return fs_utils._do_get_training_dataset_features_list(
            training_dataset, version, core._get_featurestore_metadata(featurestore, update_cache=True))


def get_training_datasets(featurestore=None):
    """
    Gets a list of all training datasets in a featurestore, will use the cached metadata

    >>> # List all Training Datasets in a Feature Store
    >>> featurestore.get_training_datasets()
    >>> # By default `get_training_datasets()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument featurestore
    >>> featurestore.get_training_datasets(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list training datasets for, defaults to the project-featurestore

    Returns:
        A list of names of the training datasets in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_datasets(core._get_featurestore_metadata(featurestore,
                                                                              update_cache=update_cache_default))
    except:
        return core._do_get_training_datasets(core._get_featurestore_metadata(featurestore, update_cache=True))


def get_project_featurestores():
    """
    Gets all featurestores for the current project

    Example usage:

    >>> # Get list of featurestores accessible by the current project example
    >>> featurestore.get_project_featurestores()

    Returns:
        A list of all featurestores that the project have access to

    """
    featurestores_json = rest_rpc._get_featurestores()
    featurestoreNames = list(map(lambda fsj: fsj[constants.REST_CONFIG.JSON_FEATURESTORE_NAME], featurestores_json))
    return featurestoreNames


def get_storage_connectors(featurestore = None):
    """
    Retrieves the names of all storage connectors in the feature store

    Example usage:

    >>> featurestore.get_storage_connectors()
    >>> # By default the query will be for the project's feature store but you can also explicitly specify the
    >>> # featurestore:
    >>> featurestore.get_storage_connector(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to query (default's to project's feature store)

    Returns:
        the storage connector with the given name
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_storage_connectors(core._get_featurestore_metadata(featurestore,
                                                                               update_cache=update_cache_default))
    except:
        return core._do_get_storage_connectors(core._get_featurestore_metadata(featurestore, update_cache=True))


def get_storage_connector(storage_connector_name, featurestore = None):
    """
    Looks up a storage connector by name

    Example usage:

    >>> featurestore.get_storage_connector("demo_featurestore_admin000_Training_Datasets")
    >>> # By default the query will be for the project's feature store but you can also explicitly specify the
    >>> # featurestore:
    >>> featurestore.get_storage_connector("demo_featurestore_admin000_Training_Datasets",
    >>>                                    featurestore=featurestore.project_featurestore())

    Args:
        :storage_connector_name: the name of the storage connector
        :featurestore: the featurestore to query (default's to project's feature store)

    Returns:
        the storage connector with the given name
    """
    if featurestore is None:
        featurestore = project_featurestore()
    return core._do_get_storage_connector(storage_connector_name, featurestore)


def get_training_dataset_path(training_dataset, featurestore=None, training_dataset_version=1):
    """
    Gets the HDFS path to a training dataset with a specific name and version in a featurestore

    Example usage:

    >>> featurestore.get_training_dataset_path("AML_dataset")
    >>> # By default the library will look for the training dataset in the project's featurestore and use version 1,
    >>> # but this can be overriden if required:
    >>> featurestore.get_training_dataset_path("AML_dataset",  featurestore=featurestore.project_featurestore(),
    >>>                                        training_dataset_version=1)

    Args:
        :training_dataset: name of the training dataset
        :featurestore: featurestore that the training dataset is linked to
        :training_dataset_version: version of the training dataset

    Returns:
        The HDFS path to the training dataset
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset_path(training_dataset,
                                                  core._get_featurestore_metadata(featurestore,
                                                                                  update_cache=update_cache_default),
                                                  training_dataset_version=training_dataset_version)
    except:
        return core._do_get_training_dataset_path(training_dataset,
                                                  core._get_featurestore_metadata(featurestore,
                                                                                  update_cache=True),
                                                  training_dataset_version=training_dataset_version)


def get_latest_training_dataset_version(training_dataset, featurestore=None):
    """
    Utility method to get the latest version of a particular training dataset

    Example usage:

    >>> featurestore.get_latest_training_dataset_version("team_position_prediction")

    Args:
        :training_dataset: the training dataset to get the latest version of
        :featurestore: the featurestore where the training dataset resides

    Returns:
        the latest version of the training dataset in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()
    return fs_utils._do_get_latest_training_dataset_version(training_dataset,
                                                            core._get_featurestore_metadata(featurestore,
                                                                                            update_cache=True))


def get_latest_featuregroup_version(featuregroup, featurestore=None):
    """
    Utility method to get the latest version of a particular featuregroup

    Example usage:

    >>> featurestore.get_latest_featuregroup_version("teams_features_spanish")

    Args:
        :featuregroup: the featuregroup to get the latest version of
        :featurestore: the featurestore where the featuregroup resides

    Returns:
        the latest version of the featuregroup in the feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()

    try:
        return fs_utils._do_get_latest_featuregroup_version(featuregroup,
                                                            core._get_featurestore_metadata(featurestore,
                                                                    update_cache=update_cache_default))
    except:
        return fs_utils._do_get_latest_featuregroup_version(featuregroup,
                                                            core._get_featurestore_metadata(featurestore,
                                                                                            update_cache=False))


def get_featuregroup_partitions(featuregroup, featurestore=None, featuregroup_version=1):
    """
    Gets the partitions of a featuregroup

    Example usage:

    >>> partitions = featurestore.get_featuregroup_partitions("trx_summary_features")
    >>> #You can also explicitly define version, featurestore and type of the returned dataframe:
    >>> featurestore.get_featuregroup_partitions("trx_summary_features",
    >>>                                          featurestore=featurestore.project_featurestore(),
    >>>                                          featuregroup_version = 1)
     Args:
        :featuregroup: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1

     Returns:
        a dataframe with the partitions of the featuregroup
     """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Try with cached metadata
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore,
                                                                                    update_cache=update_cache_default),
                                                    featurestore, featuregroup_version)
    except:
        # Retry with updated cache
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=True),
                                                    featurestore, featuregroup_version)

def import_featuregroup_s3(storage_connector, featuregroup, path=None, primary_key=[], description="",
                           featurestore=None, featuregroup_version=1, jobs=[], descriptive_statistics=True,
                           feature_correlation=True, feature_histograms=True, cluster_analysis=True, stat_columns=None,
                           num_bins=20, corr_method='pearson', num_clusters=5, partition_by=[], data_format="parquet",
                           online=False, online_types=None, offline=True,
                           am_cores=1, am_memory=2048, executor_cores=1, executor_memory=4096, max_executors=2):

    """
    Creates and triggers a job to import an external dataset of features into a feature group in Hopsworks.
    This function will read the dataset using spark and a configured s3 storage connector
    and then writes the data to Hopsworks Feature Store (Hive) and registers its metadata.

    Example usage:

    >>> featurestore.import_featuregroup(my_s3_connector_name, s3_path, featuregroup_name,
    >>>                                  data_format=s3_bucket_data_format)
    >>> # You can also be explicitly specify featuregroup metadata and what statistics to compute:
    >>> featurestore.import_featuregroup(my_s3_connector_name, s3_path, featuregroup_name, primary_key=["id"],
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[], data_format="parquet", am_cores=1,
    >>>                                  online=False, online_types=None, offline=True,
    >>>                                  am_memory=2048, executor_cores=1, executor_memory=4096, max_executors=2)

    Args:
        :storage_connector: the storage connector used to connect to the external storage
        :path: the path to read from the external storage
        :featuregroup: name of the featuregroup to import the dataset into the featurestore
        :primary_key: a list of columns to be used as primary key of the new featuregroup, if not specified,
            the first column in the dataframe will be used as primary
        :description: metadata description of the feature group to import
        :featurestore: name of the featurestore database to import the feature group into
        :featuregroup_version: version of the feature group
        :jobs: list of Hopsworks jobs linked to the feature group (optional)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :num_clusters: the number of clusters to use for cluster analysis
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :data_format: the format of the external dataset to read
        :online: boolean flag, if this is set to true, a MySQL table for online feature data will be created in
                 addition to the Hive table for offline feature data
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                       the online_type will be taken from the dict rather than inferred from the spark dataframe.
        :offline: boolean flag whether to insert the data in the offline version of the featuregroup
        :am_cores: number of cores for the import job's application master
        :am_memory: ammount of memory for the import job's application master
        :executor_cores: number of cores for the import job's executors
        :executor_memory: ammount of memory for the import job's executors
        :max_executors: max number of executors to allocate to the spark dinamic app.

    Returns:
        None

    Raises:
        :StorageConnectorNotFound: when the requested storage connector could not be found in the metadata
    """
    # Deprecation warning
    if isinstance(primary_key, str):
        print(
            "DeprecationWarning: Primary key of type str is deprecated. With the introduction of composite primary keys"
            " this method expects a list of strings to define the primary key.")
        primary_key = [primary_key]
    # try getting the storage connector to check for its existence, throws StorageConnectorNotFound
    core._do_get_storage_connector(storage_connector, featurestore)
    try:
        fs_utils._validate_metadata(
            featuregroup, description, core._get_featurestore_metadata(
                featurestore, update_cache=update_cache_default).settings)
    except: # retry with updated metadata
        fs_utils._validate_metadata(
            featuregroup, description, core._get_featurestore_metadata(
                featurestore, update_cache=True).settings)
    arguments = locals()
    arguments['type'] = "S3"
    core._do_import_featuregroup(json.dumps(arguments))
    #path to json file in hdfs
    input_json_path = '--job_spec hdfs:///Projects/' + \
                      os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] + \
                      '/Resources/featurestore_import/configurations/' + \
                      featuregroup + '.json'
    job.launch_job(featuregroup, input_json_path)

def import_featuregroup_redshift(storage_connector, query, featuregroup, primary_key=[], description="",
                                 featurestore=None, featuregroup_version=1, jobs=[], descriptive_statistics=True,
                                 feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                 stat_columns=None, num_bins=20, corr_method='pearson', num_clusters=5,
                                 partition_by=[], online=False, online_types=None, offline=True,
                                 am_cores=1, am_memory=2048, executor_cores=1, executor_memory=4096, max_executors=2):
    """
    Creates and triggers a job to import an external dataset of features into a feature group in Hopsworks.
    This function will read the dataset using spark and a configured redshift storage connector
    and then writes the data to Hopsworks Feature Store (Hive) and registers its metadata.

    Example usage:

    >>> featurestore.import_featuregroup_redshift(my_jdbc_connector_name, sql_query, featuregroup_name)
    >>> # You can also be explicitly specify featuregroup metadata and what statistics to compute:
    >>> featurestore.import_featuregroup_redshift(my_jdbc_connector_name, sql_query, featuregroup_name, primary_key=["id"],
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(), featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[], online=False, online_types=None, offline=True,
    >>>                                  am_cores=1, am_memory=2048, executor_cores=1, executor_memory=4096, max_executors=2)

    Args:
        :storage_connector: the storage connector used to connect to the external storage
        :query: the queury extracting data from Redshift
        :featuregroup: name of the featuregroup to import the dataset into the featurestore
        :primary_key: a list columns to be used as primary key of the new featuregroup, if not specified,
            the first column in the dataframe will be used as primary
        :description: metadata description of the feature group to import
        :featurestore: name of the featurestore database to import the feature group into
        :featuregroup_version: version of the feature group
        :jobs: list of Hopsworks jobs linked to the feature group (optional)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :num_clusters: the number of clusters to use for cluster analysis
        :partition_by: a list of columns to partition_by, defaults to the empty list
        :online: boolean flag, if this is set to true, a MySQL table for online feature data will be created in
                 addition to the Hive table for offline feature data
        :online_types: a dict with feature_name --> online_type, if a feature is present in this dict,
                       the online_type will be taken from the dict rather than inferred from the spark dataframe.
        :offline: boolean flag whether to insert the data in the offline version of the featuregroup
        :am_cores: number of cores for the import job's application master
        :am_memory: ammount of memory for the import job's application master
        :executor_cores: number of cores for the import job's executors
        :executor_memory: ammount of memory for the import job's executors
        :max_executors: max number of executors to allocate to the spark dinamic app.


    Returns:
        None

    Raises:
        :StorageConnectorNotFound: when the requested storage connector could not be found in the metadata
    """
    # Deprecation warning
    if isinstance(primary_key, str):
        print(
            "DeprecationWarning: Primary key of type str is deprecated. With the introduction of composite primary keys"
            " this method expects a list of strings to define the primary key.")
        primary_key = [primary_key]
    # try getting the storage connector to check for its existence, throws StorageConnectorNotFound
    core._do_get_storage_connector(storage_connector, featurestore)
    try:
        fs_utils._validate_metadata(
            featuregroup, description, core._get_featurestore_metadata(
                featurestore, update_cache=update_cache_default).settings)
    except: # retry with updated metadata
        fs_utils._validate_metadata(
            featuregroup, description, core._get_featurestore_metadata(
                featurestore, update_cache=True).settings)
    arguments = locals()
    arguments['type'] = "REDSHIFT"
    core._do_import_featuregroup(json.dumps(arguments))
    #path to json file in hdfs
    input_json_path = '--job_spec hdfs:///Projects/' + \
                      os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] + \
                      '/Resources/featurestore_import/configurations/' + \
                      featuregroup + '.json'
    job.launch_job(featuregroup, input_json_path)


def connect(host, project_name, port = 443, region_name = constants.AWS.DEFAULT_REGION,
            secrets_store = 'parameterstore', hostname_verification=True, trust_store_path=None,
            use_metadata_cache=False, cert_folder='', api_key_file=None):
    """
    Connects to a feature store from a remote environment such as Amazon SageMaker

    Example usage:

    >>> featurestore.connect("hops.site", "my_feature_store")

    Args:
        :host: the hostname of the Hopsworks cluster
        :project_name: the name of the project hosting the feature store to be used
        :port: the REST port of the Hopsworks cluster
        :region_name: The name of the AWS region in which the required secrets are stored
        :secrets_store: The secrets storage to be used. Either secretsmanager, parameterstore or local
        :hostname_verification: Enable or disable hostname verification. If a self-signed certificate was installed \
        on Hopsworks then the trust store needs to be supplied using trust_store_path.
        :trust_store_path: the trust store pem file for Hopsworks needed for self-signed certificates only
        :use_metadata_cache: Whether the metadata cache should be used or not. If enabled some API calls may return \
        outdated data.
        :cert_folder: the folder in which to store the Hopsworks certificates.
        :api_key_file: path to a file containing an API key. For secrets_store=local only.

    Returns:
        None
    """
    global update_cache_default
    update_cache_default = not use_metadata_cache

    os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = host + ':' + str(port)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project_name
    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = util.get_secret(secrets_store, 'api-key', api_key_file)

    util.prepare_requests(hostname_verification=hostname_verification, trust_store_path=trust_store_path)

    project_info = rest_rpc._get_project_info(project_name)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id

    credentials = rest_rpc._get_credentials(project_id)
    util.write_b64_cert_to_bytes(str(credentials['kStore']), path=os.path.join(cert_folder, 'keyStore.jks'))
    util.write_b64_cert_to_bytes(str(credentials['tStore']), path=os.path.join(cert_folder, 'trustStore.jks'))

    os.environ[constants.ENV_VARIABLES.CERT_FOLDER_ENV_VAR] = cert_folder
    os.environ[constants.ENV_VARIABLES.CERT_KEY_ENV_VAR] = str(credentials['password'])

def get_online_featurestore_connector(featurestore=None):
    """
    Gets a JDBC connector for the online feature store
    Args:
        :featurestore: the feature store name
    Returns:
        a DTO object of the JDBC connector for the online feature store
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try: # try with metadata cache
        if update_cache_default:
            core._get_featurestore_metadata(featurestore, update_cache=True)
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore,
                                                                                   update_cache=update_cache_default))
    except: # retry with updated metadata
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore, update_cache=True))

def create_training_dataset(training_dataset, features=None, sql_query=None, featurestore=None,
                            featuregroups_version_dict={}, join_key=None, description="", data_format="tfrecords",
                            training_dataset_version=1, overwrite=False, jobs=[], online=False,
                            fixed=True, sink=None, path=None, am_cores=1, am_memory=2048,
                            executor_cores=1, executor_memory=4096, max_executors=2):
    """
    Creates and triggers a job to create a training dataset of features from a featurestore in Hopsworks.
    The function joins the features on the specified `join_key`, saves metadata about the training dataset to the database
    and saves the materialized dataset to the storage connector provided in `sink`. A custom sink can be defined, by
    adding a storage connector in Hopsworks. The job is executed in Spark with a dynamically scaled number of executors
    up to `max_executors` according to the availability of resources.

    >>> featurestore.create_training_dataset(["feature1", "feature2", "label"], "TestDataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(["feature1", "feature2", "label"], "TestDataset", description="",
    >>>                                      featurestore=featurestore.project_featurestore(), data_format="csv",
    >>>                                      training_dataset_version=1, sink = "s3_connector")

    Args:
        :training_dataset: The name of the training dataset.
        :features: A list of features, to be added to the training dataset. `features` or `sql_query`, one of the two
            should not be `None`. Defaults to `None`.
        :sql_query: A generic SQL query string to create a training dataset from the featurestore. Be aware that no query
            validation is performed until the job is being run, and hence the job might fail if your SQL string is
            mal-formed. If `sql_query` is provided, `join_key` and `featuregroups_version_dict` become obsolete.
            `features` or `sql_query`, one of the two should not be `None`. Defaults to `None`.
        :featurestore: The name of the featurestore that the training dataset is linked to.
                       Defaults to None, using the project default featurestore.
        :featuregroups_version_dict: An optional dict with (fg --> version) for all the featuregroups where the features reside.
                                     Hopsworks will try to infer the featuregroup version from metadata. Defaults to {}.
        :join_key: (Optional) column name to join on. Defaults to None.
        :description: A description of the training dataset. Defaults to "".
        :data_format: The format of the materialized training dataset. Defaults to "tfrecords".
        :training_dataset_version: The version of the training dataset. Defaults to 1.
        :overwrite: Boolean to indicate if an existing training dataset with the same version should be overwritten. Defaults to False.
        :jobs: List of Hopsworks jobs linked to the training dataset. Defaults to [].
        :online: Boolean flag whether to run the query against the online featurestore (otherwise it will be the offline
            featurestore).
        :fixed: Boolean flag indicating whether array columns should be treated with fixed size or variable size. Defaults to True.
        :sink: Name of storage connector to store the training dataset. Defaults to the hdfs connector.
        :path: path to complement the sink storage connector with, e.g if the storage connector points to an
               S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training
               dataset.
        :am_cores: Number of cores assigned to the application master of the job. Defaults to 1.
        :am_memory: Memory in MB assigned to the application master of the job. Defaults to 2048.
        :executor_cores: Number of cores assigned to each of the executors of the job. Defaults to 1.
        :executor_memory: Memory in MB assigned to each of the executors of the job. Defaults to 4096.
        :max_executors: Maximum number of executors assigned to the job.

    Raises:
        :StorageConnectorNotFound: when the requested storage connector could not be found in the metadata
    """
    # try getting the storage connector to check for its existence, throws StorageConnectorNotFound
    if sink:
        core._do_get_storage_connector(sink, featurestore)
    try:
        fs_utils._validate_metadata(
            training_dataset, description, core._get_featurestore_metadata(
                featurestore, update_cache=update_cache_default).settings)
    except: # retry with updated metadata
        fs_utils._validate_metadata(
            training_dataset, description, core._get_featurestore_metadata(
                featurestore, update_cache=True).settings)
    job_conf = locals()
    # treat featuregroups_version_dict as string
    job_conf['featuregroups_version_dict'] = json.dumps(job_conf['featuregroups_version_dict'])
    core._do_trainingdataset_create(json.dumps(job_conf))
    #path to json file in hdfs
    input_json_path = '--job_spec hdfs:///Projects/' + \
                      os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] + \
                      '/Resources/featurestore-trainingdataset-job/configurations/' + \
                      training_dataset + '.json'
    job.launch_job(training_dataset, input_json_path)
    print('Training Dataset job successfully started')

def add_metadata(featuregroup_name, metadata, featuregroup_version=1, featurestore=None):
    """
    Attach custom metadata to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> featurestore.add_metadata(featuregroup_name, {"attr1" : "val1", "attr2" : "val2"})
    >>> # You can also explicitly override the default arguments:
    >>> featurestore.add_metadata(featuregroup_name, {"attr1" : "val1", "attr2" : "val2"}, featuregroup_version=1, featurestore=featurestore)

    Args:
        :featuregroup_name: name of the featuregroup
        :metadata: a dictionary of attributes to attach to the featuregroup
        :featuregroup_version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        None
    """
    if type(metadata) is not dict:
        raise ValueError("metadata should be a dictionary")

    for k, v in metadata.items():
        core._do_add_metadata(featuregroup_name, k, v, featurestore, featuregroup_version)


def get_metadata(featuregroup_name, keys=[], featuregroup_version=1, featurestore=None):
    """
    Gets the custom metadata attached to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> metadata = featurestore.get_metadata(featuregroup_name, ["attr1", "attr2"])
    >>> # The API will return all associated metadata if no keys are supplied
    >>> metadata = featurestore.get_metadata(featuregroup_name)
    >>> # You can also explicitly override the default arguments:
    >>> metadata = featurestore.get_metadata(featuregroup_name, ["attr1", "attr2"], featuregroup_version=1, featurestore=featurestore)

    Args:
        :featuregroup_name: name of the featuregroup
        :keys: array of attribute names to read for the featuregroup associated metadata
        :featuregroup_version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        The metadata dictionary attached to the featuregroup
    """
    if not keys:
        return core._do_get_metadata(featuregroup_name=featuregroup_name, name=None, featurestore=featurestore, featuregroup_version=featuregroup_version)

    if type(keys) is not list:
        raise ValueError("keys should be a list")

    vals = {}
    for k in keys:
        vals.update(core._do_get_metadata(featuregroup_name, k, featurestore, featuregroup_version))
    return vals


def remove_metadata(featuregroup_name, keys, featuregroup_version=1, featurestore=None):
    """
    Removes the custom metadata attached to a feature group

    Example usage:

    >>> # The API will default to the project's feature store
    >>> metadata = featurestore.remove_metadata(featuregroup_name, ["attr1", "attr2"])
    >>> # You can also explicitly override the default arguments:
    >>> metadata = featurestore.remove_metadata(featuregroup_name, ["attr1", "attr2"], featuregroup_version=1, featurestore=featurestore)

    Args:
        :featuregroup_name: name of the featuregroup
        :keys: array of attribute names to be deleted from the featuregroup associated metadata
        :featuregroup_version: version of the featuregroup
        :featurestore: the featurestore that the featuregroup belongs to

    Returns:
        None
    """
    if type(keys) is not list:
        raise ValueError("keys should be a list")

    for k in keys:
        core._do_remove_metadata(featuregroup_name, k, featurestore, featuregroup_version)