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

from hops import util, constants
from hops.featurestore_impl import core
from hops.featurestore_impl.exceptions.exceptions import FeatureVisualizationError
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils


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
                                         core._get_featurestore_metadata(featurestore, update_cache=False),
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
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=False),
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
                                     core._get_featurestore_metadata(featurestore, update_cache=False),
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
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=False),
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
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=False,),
                                              online=online)
    except:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=True,),
                                              online=online)


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
        return core._do_get_training_datasets(core._get_featurestore_metadata(featurestore, update_cache=False))
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
        return core._do_get_storage_connectors(core._get_featurestore_metadata(featurestore, update_cache=False))
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
                                                                                  update_cache=False),
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
    try:
        return fs_utils._do_get_latest_training_dataset_version(training_dataset,
                                                                core._get_featurestore_metadata(featurestore,
                                                                                                update_cache=False))
    except:
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
                                                                                            update_cache=False))
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
                                                    core._get_featurestore_metadata(featurestore, update_cache=False),
                                                    featurestore, featuregroup_version)
    except:
        # Retry with updated cache
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=True),
                                                    featurestore, featuregroup_version)


def visualize_featuregroup_distributions(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=None,
                                         color='lightblue', log=False, align="center", plot=True):
    """
    Visualizes the feature distributions (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_distributions("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_distributions("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  color="lightblue",
    >>>                                                  figsize=(16,12),
    >>>                                                  log=False,
    >>>                                                  align="center",
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: size of the figure
        :figsize: the size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature distributions
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_featuregroup_distributions(featuregroup_name, featurestore, featuregroup_version,
                                                            figsize=figsize, color=color, log=log, align=align)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_featuregroup_distributions(featuregroup_name, featurestore, featuregroup_version,
                                                                figsize=figsize, color=color, log=log, align=align)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature distributions for "
                                            "feature group: {} with version: {} in featurestore: {}. Error: {}".format(
                featuregroup_name, featuregroup_version, featurestore, str(e)))


def visualize_featuregroup_correlations(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16,12),
                                        cmap="coolwarm", annot=True, fmt=".2f", linewidths=.05, plot=True):
    """
    Visualizes the feature correlations (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_correlations("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_correlations("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  cmap="coolwarm",
    >>>                                                  figsize=(16,12),
    >>>                                                  annot=True,
    >>>                                                  fmt=".2f",
    >>>                                                  linewidths=.05
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature correlations
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_featuregroup_correlations(featuregroup_name, featurestore, featuregroup_version,
                                                            figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                           linewidths=linewidths)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_featuregroup_correlations(featuregroup_name, featurestore, featuregroup_version,
                                                               figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                               linewidths=linewidths)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature correlations for "
                                            "feature group: {} with version: {} in featurestore: {}. Error: {}".format(
                featuregroup_name, featuregroup_version, featurestore, str(e)))


def visualize_featuregroup_clusters(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16,12),
                                    plot=True):
    """
    Visualizes the feature clusters (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_clusters("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_clusters("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1,
    >>>                                                  figsize=(16,12),
    >>>                                                  plot=True)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature clusters
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_featuregroup_clusters(featuregroup_name, featurestore, featuregroup_version,
                                                           figsize=figsize)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_featuregroup_clusters(featuregroup_name, featurestore, featuregroup_version,
                                                               figsize=figsize)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature clusters for "
                                            "feature group: {} with version: {} in featurestore: {}. Error: {}".format(
                featuregroup_name, featuregroup_version, featurestore, str(e)))


def visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    Visualizes the descriptive stats (if they have been computed) for a featuregroup in the featurestore

    Example usage:

    >>> featurestore.visualize_featuregroup_descriptive_stats("trx_summary_features")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_featuregroup_descriptive_stats("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
        A pandas dataframe with the descriptive statistics

    Raises:
        :FeatureVisualizationError: if there was an error in fetching the descriptive statistics
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        df = core._do_visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore,
                                                                   featuregroup_version)
        return df
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            df = core._do_visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore,
                                                                       featuregroup_version)
            return df

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the descriptive statistics for "
                                            "featuregroup: {} with version: {} in featurestore: {}. "
                                            "Error: {}".format(featuregroup_name, featuregroup_version,
                                                               featurestore, str(e)))


def visualize_training_dataset_distributions(training_dataset_name, featurestore=None, training_dataset_version=1,
                                             figsize=(16, 12), color='lightblue', log=False, align="center", plot=True):
    """
    Visualizes the feature distributions (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_distributions("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_distributions("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  color="lightblue",
    >>>                                                  figsize=(16,12),
    >>>                                                  log=False,
    >>>                                                  align="center",
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: size of the figure
        :figsize: the size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature distributions
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_training_dataset_distributions(training_dataset_name, featurestore,
                                                            training_dataset_version, figsize=figsize, color=color,
                                                            log=log, align=align)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_training_dataset_distributions(training_dataset_name, featurestore,
                                                                training_dataset_version, figsize=figsize, color=color,
                                                                log=log, align=align)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature distributions for "
                                            "training dataset: {} with version: {} in featurestore: {}. "
                                            "Error: {}".format(training_dataset_name, training_dataset_version,
                                                               featurestore, str(e)))


def visualize_training_dataset_correlations(training_dataset_name, featurestore=None, training_dataset_version=1,
                                            figsize=(16,12), cmap="coolwarm", annot=True, fmt=".2f",
                                            linewidths=.05, plot=True):
    """
    Visualizes the feature distributions (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_correlations("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_correlations("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  cmap="coolwarm",
    >>>                                                  figsize=(16,12),
    >>>                                                  annot=True,
    >>>                                                  fmt=".2f",
    >>>                                                  linewidths=.05
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature correlations
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_training_dataset_correlations(training_dataset_name, featurestore,
                                                               training_dataset_version, figsize=figsize, cmap=cmap,
                                                               annot=annot, fmt=fmt, linewidths=linewidths)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_training_dataset_correlations(training_dataset_name, featurestore,
                                                                   training_dataset_version, figsize=figsize,
                                                                   cmap=cmap, annot=annot, fmt=fmt,
                                                                   linewidths=linewidths)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature correlations for "
                                            "training dataset: {} with version: {} in featurestore: {}. "
                                            "Error: {}".format(training_dataset_name, training_dataset_version,
                                                               featurestore, str(e)))


def visualize_training_dataset_clusters(training_dataset_name, featurestore=None, training_dataset_version=1,
                                        figsize=(16,12), plot=True):
    """
    Visualizes the feature clusters (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_clusters("AML_dataset")
    >>> # You can also explicitly define version, featurestore and plotting options
    >>> featurestore.visualize_training_dataset_clusters("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1,
    >>>                                                  figsize=(16,12),
    >>>                                                  plot=True)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :plot: if set to True it will plot the image and return None, if set to False it will not plot it
               but rather return the figure

    Returns:
        if the 'plot' flag is set to True it will plot the image and return None, if the 'plot' flag is set to False
        it will not plot it but rather return the figure

    Raises:
        :FeatureVisualizationError: if there was an error visualizing the feature clusters
    """
    if plot:
        fs_utils._visualization_validation_warning()

    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Construct the figure
        fig = core._do_visualize_training_dataset_clusters(training_dataset_name, featurestore,
                                                           training_dataset_version, figsize=figsize)
        if plot:
            # Plot the figure
            fig.tight_layout()
        else:
            return fig
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            # Construct the figure
            fig = core._do_visualize_training_dataset_clusters(training_dataset_name, featurestore,
                                                               training_dataset_version, figsize=figsize)
            if plot:
                # Plot the figure
                fig.tight_layout()
            else:
                return fig

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the feature clusters for "
                                            "training dataset: {} with version: {} in featurestore: {}. "
                                            "Error: {}".format(training_dataset_name, training_dataset_version,
                                                               featurestore, str(e)))


def visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    Visualizes the descriptive stats (if they have been computed) for a training dataset in the featurestore

    Example usage:

    >>> featurestore.visualize_training_dataset_descriptive_stats("AML_dataset")
    >>> # You can also explicitly define version and featurestore
    >>> featurestore.visualize_training_dataset_descriptive_stats("AML_dataset",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  training_dataset_version = 1)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
        A pandas dataframe with the descriptive statistics

    Raises:
        :FeatureVisualizationError: if there was an error in fetching the descriptive statistics
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        df = core._do_visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore,
                                                                   training_dataset_version)
        return df
    except:
        # Retry with updated cache
        core._get_featurestore_metadata(featurestore, update_cache=True)
        try:
            df = core._do_visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore,
                                                                       training_dataset_version)
            return df

        except Exception as e:
            raise FeatureVisualizationError("There was an error in visualizing the descriptive statistics for "
                                            "training dataset: {} with version: {} in featurestore: {}. "
                                            "Error: {}".format(training_dataset_name, training_dataset_version,
                                                               featurestore, str(e)))


def get_featuregroup_statistics(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    Gets the computed statistics (if any) of a featuregroup

    Example usage:

    >>> stats = featurestore.get_featuregroup_statistics("trx_summary_features")
    >>> # You can also explicitly define version and featurestore
    >>> stats = featurestore.get_featuregroup_statistics("trx_summary_features",
    >>>                                                  featurestore=featurestore.project_featurestore(),
    >>>                                                  featuregroup_version = 1)

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
          A Statistics Object
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_featuregroup_statistics(featuregroup_name, featurestore, featuregroup_version)
    except:
        core._get_featurestore_metadata(featurestore, update_cache=True)
        return core._do_get_featuregroup_statistics(featuregroup_name, featurestore, featuregroup_version)


def get_training_dataset_statistics(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    Gets the computed statistics (if any) of a training dataset

    Example usage:

    >>> stats = featurestore.get_training_dataset_statistics("AML_dataset")
    >>> # You can also explicitly define version and featurestore
    >>> stats = featurestore.get_training_dataset_statistics("AML_dataset",
    >>>                                                      featurestore=featurestore.project_featurestore(),
    >>>                                                      training_dataset_version = 1)

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
          A Statistics Object
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset_statistics(training_dataset_name, featurestore, training_dataset_version)
    except:
        core._get_featurestore_metadata(featurestore, update_cache=True)
        return core._do_get_training_dataset_statistics(training_dataset_name, featurestore, training_dataset_version)

def connect(host, project_name, port = 443, region_name = constants.AWS.DEFAULT_REGION,
            secrets_store = 'parameterstore', hostname_verification=True):
    """
    Connects to a feature store from a remote environment such as Amazon SageMaker

    Example usage:

    >>> featurestore.connect("hops.site", "my_feature_store")

    Args:
        :host: the hostname of the Hopsworks cluster
        :project_name: the name of the project hosting the feature store to be used
        :port: the REST port of the Hopsworks cluster
        :region_name: The name of the AWS region in which the required secrets are stored
        :secrets_store: The secrets storage to be used. Either secretsmanager or parameterstore.
        :hostname_verification: Enable or disable hostname verification

    Returns:
        None
    """

    if hostname_verification:
        os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = 'true'
    else:
        os.environ[constants.ENV_VARIABLES.REQUESTS_VERIFY_ENV_VAR] = 'false'

    os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = host + ':' + str(port)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project_name
    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    os.environ[constants.ENV_VARIABLES.API_KEY_ENV_VAR] = util.get_secret(util.project_name(), secrets_store, 'api-key')
    project_info = rest_rpc._get_project_info(project_name)
    project_id = str(project_info['projectId'])
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = project_id

    # download certificates from AWS Secret manager to access Hive
    credentials = rest_rpc._get_credentials(project_id)
    util.write_b64_cert_to_bytes(str(credentials['kStore']), path='keyStore.jks')
    util.write_b64_cert_to_bytes(str(credentials['tStore']), path='trustStore.jks')

    # write env variables
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
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore, update_cache=False))
    except: # retry with updated metadata
        return core._do_get_online_featurestore_connector(featurestore,
                                                   core._get_featurestore_metadata(featurestore, update_cache=True))
