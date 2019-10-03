"""
Featurestore Core Implementation

Module hierarchy of featurestore implementation:

- featurestore
       |
       --- core
             |
             ----dao
             ----exceptions
             ----query_planner
             ----rest
             ----util
             ----featureframes
             ----visualizations
"""
import urllib

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import NullPool

from hops import constants, util
from hops.featurestore_impl.dao.common.featurestore_metadata import FeaturestoreMetadata
from hops.featurestore_impl.dao.stats.statistics import Statistics
from hops.featurestore_impl.dao.storageconnectors.jdbc_connector import JDBCStorageConnector
from hops.featurestore_impl.exceptions.exceptions import FeaturegroupNotFound, TrainingDatasetNotFound, \
    FeatureDistributionsNotComputed, \
    FeatureCorrelationsNotComputed, FeatureClustersNotComputed, DescriptiveStatisticsNotComputed, \
    StorageConnectorNotFound, CannotGetPartitionsOfOnDemandFeatureGroup
from hops.featurestore_impl.online_featurestore import _get_online_feature_store_password_and_user
from hops.featurestore_impl.query_planner import query_planner
from hops.featurestore_impl.query_planner.f_query import FeatureQuery, FeaturesQuery
from hops.featurestore_impl.query_planner.fg_query import FeaturegroupQuery
from hops.featurestore_impl.query_planner.logical_query_plan import LogicalQueryPlan
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl.visualizations import statistics_plots

metadata_cache = None


def _get_featurestore_id(featurestore):
    """
    Gets the id of a featurestore (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore to get the id for

    Returns:
        the id of the feature store

    """
    if metadata_cache is None or featurestore != metadata_cache.featurestore:
        _get_featurestore_metadata(featurestore, update_cache=True)
    return metadata_cache.featurestore.id


def _get_featurestore_metadata(featurestore=None, update_cache=False):
    """
    Makes a REST call to the appservice in hopsworks to get all metadata of a featurestore (featuregroups and
    training datasets) for the provided featurestore.
    Args:
        :featurestore: the name of the database, defaults to the project's featurestore
        :update_cache: if true the cache is updated
    Returns:
        feature store metadata object
    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    global metadata_cache
    if metadata_cache is None or update_cache:
        response_object = rest_rpc._get_featurestore_metadata(featurestore)
        metadata_cache = FeaturestoreMetadata(response_object)
    return metadata_cache


def _get_featuregroup_id(featurestore, featuregroup_name, featuregroup_version):
    """
    Gets the id of a featuregroup (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore where the featuregroup belongs
        :featuregroup: the featuregroup to get the id for
        :featuregroup_version: the version of the featuregroup

    Returns:
        the id of the featuregroup

    Raises:
        :FeaturegroupNotFound: when the requested featuregroup could not be found in the metadata
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    for fg in metadata.featuregroups.values():
        if fg.name == featuregroup_name \
                and fg.version == featuregroup_version:
            return fg.id
    raise FeaturegroupNotFound("The featuregroup {} with version: {} "
                               "was not found in the feature store {}".format(featuregroup_name, featuregroup_version,
                                                                              featurestore))


def _do_get_storage_connector(storage_connector_name, featurestore):
    """
    Looks up the metadata of a storage connector given a name

    Args:
        :storage_connector_name: the storage connector name
        :featurestore: the featurestore to query

    Returns:
        the id of the featuregroup

    Raises:
        :FeaturegroupNotFound: when the requested featuregroup could not be found in the metadata
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    try:
        return metadata.storage_connectors[storage_connector_name]
    except:
        try:
            # Retry with updated metadata
            metadata = _get_featurestore_metadata(
                featurestore, update_cache=True)
        except KeyError:
            storage_connector_names = list(
                map(lambda sc: sc.name, metadata.storage_connectors))
            raise StorageConnectorNotFound("Could not find the requested storage connector with name: {} "
                                           ", among the list of available storage connectors: {}".format(
                                               storage_connector_name,
                                               storage_connector_names))


def _do_get_feature(feature, featurestore_metadata, featurestore=None, featuregroup=None, featuregroup_version=1,
                    online=False):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries
    hopsworks metastore to see if the feature exists in any of the featuregroups in the featurestore.
    If the user knows which featuregroup contain the feature, it should be specified as it will improve performance
    of the query.

    Args:
        :feature: the feature name to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup: (Optional) the featuregroup where the feature resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :featurestore_metadata: the metadata of the featurestore to query
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        A pandas dataframe with the feature

    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    feature_query = FeatureQuery(
        feature, featurestore_metadata, featurestore, featuregroup, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(feature_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()

    dataframe = _run_and_log_sql(
        logical_query_plan.sql_str, featurestore, online)
    return dataframe


def _run_and_log_sql(sql_str, featurestore, online=False):
    """
    Runs and logs an SQL query with pyHive

    Args:
        :sql_str: the query to run
        :featurestore: name of the featurestore
        :online: if true, run the query using online feature store JDBC connector

    Returns:
        :pd.DataFrame: the result of the SQL query as pandas dataframe
    """
    if not online:
        hive_conn = None
        try:
            fs_utils._log(
                "Running sql: {} against the offline feature store".format(sql_str))
            hive_conn = util._create_hive_connection(featurestore)
            dataframe = pd.read_sql(sql_str, hive_conn)
        finally:
            if hive_conn:
                hive_conn.close()
    else:
        connection = None
        try:
            fs_utils._log(
                "Running sql: {} against online feature store".format(sql_str))
            metadata = _get_featurestore_metadata(
                featurestore, update_cache=False)
            storage_connector = _do_get_online_featurestore_connector(
                featurestore, metadata)
            pw, user = _get_online_feature_store_password_and_user(
                storage_connector)
            parsed = urllib.parse.urlparse(urllib.parse.urlparse(
                storage_connector.connection_string).path)
            db_connection_str = 'mysql+pymysql://' + user + \
                ':' + pw + '@' + parsed.netloc + parsed.path
            engine = sqlalchemy.create_engine(
                db_connection_str, poolclass=NullPool)
            db_connection = engine.connect()
            dataframe = pd.read_sql(sql_str, con=db_connection)
        finally:
            if connection:
                connection.close()

    # pd.read_sql returns columns in table.column format if columns are not specified in SQL query, i.e. SELECT * FROM..
    # this also occurs when sql query specifies table, i.e. SELECT table1.column1 table2.column2 FROM ... JOIN ...
    # we want only want hive table column names as dataframe column names
    dataframe.columns = [column.split(
        '.')[1] if '.' in column else column for column in dataframe.columns]

    return dataframe


def _do_get_features(features, featurestore_metadata, featurestore=None, featuregroups_version_dict={}, join_key=None,
                     online=False):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored.

    Args:
        :features: a list of features to get from the featurestore
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        :featuregroup_version: (Optional) the version of the featuregroup
        :join_key: (Optional) column name to join on
        :featurestore_metadata: the metadata of the featurestore
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        A pandas dataframe with all the features

    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    features_query = FeaturesQuery(
        features, featurestore_metadata, featurestore, featuregroups_version_dict, join_key)
    logical_query_plan = LogicalQueryPlan(features_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()

    result = _run_and_log_sql(logical_query_plan.sql_str, featurestore, online)

    return result


def _do_get_featuregroup(featuregroup_name, featurestore_metadata, featurestore=None, featuregroup_version=1, online=False):
    """
    Gets a featuregroup from a featurestore as a pandas dataframe

    Args:
        :featuregroup_name: name of the featuregroup to get
        :featurestore_metadata: featurestore metadata
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        a pandas dataframe with the contents of the featurestore

    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()
    fg = query_planner._find_featuregroup(
        featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)

    if fg.featuregroup_type == featurestore_metadata.settings.cached_featuregroup_type:
        return _do_get_cached_featuregroup(featuregroup_name, featurestore, featuregroup_version, online)

    raise ValueError("The feature group type: "
                     + fg.featuregroup_type + " was not recognized. Recognized types include: {} and {}"
                     .format(featurestore_metadata.settings.on_demand_featuregroup_type,
                             featurestore_metadata.settings.cached_featuregroup_type))


def _do_get_cached_featuregroup(featuregroup_name, featurestore=None, featuregroup_version=1, online=False):
    """
    Gets a cached featuregroup from a featurestore as a pandas dataframe

    Args:
        :featuregroup_name: name of the featuregroup to get
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: (Optional) the version of the featuregroup
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

    Returns:
        a pandas dataframe with the contents of the feature group

    """
    if featurestore is None:
        featurestore = fs_utils._do_get_project_featurestore()

    featuregroup_query = FeaturegroupQuery(
        featuregroup_name, featurestore, featuregroup_version)
    logical_query_plan = LogicalQueryPlan(featuregroup_query)
    logical_query_plan.create_logical_plan()
    logical_query_plan.construct_sql()
    dataframe = _run_and_log_sql(
        logical_query_plan.sql_str, featurestore=featurestore, online=online)
    return dataframe


def _get_training_dataset_id(featurestore, training_dataset_name, training_dataset_version):
    """
    Gets the id of a training_Dataset (temporary workaround until HOPSWORKS-860 where we use Name to refer to resources)

    Args:
        :featurestore: the featurestore where the featuregroup belongs
        :training_dataset_name: the training_dataset to get the id for
        :training_dataset_version: the id of the training dataset

    Returns:
        the id of the training dataset

    Raises:
        :TrainingDatasetNotFound: if the requested trainining dataset could not be found
    """
    metadata = _get_featurestore_metadata(featurestore, update_cache=False)
    if metadata is None or featurestore != metadata.featurestore.name:
        metadata = _get_featurestore_metadata(featurestore, update_cache=True)
    for td in metadata.training_datasets.values():
        if td.name == training_dataset_name and td.version == training_dataset_version:
            return td.id
    raise TrainingDatasetNotFound("The training dataset {} with version: {} "
                                  "was not found in the feature store {}".format(
                                      training_dataset_name, training_dataset_version, featurestore))


def _do_get_training_datasets(featurestore_metadata):
    """
    Gets a list of all training datasets in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore

    Returns:
        A list of names of the training datasets in this featurestore
    """
    training_dataset_names = list(
        map(lambda td: fs_utils._get_table_name(td.name,
                                                td.version),
            featurestore_metadata.training_datasets.values()))
    return training_dataset_names


def _do_get_storage_connectors(featurestore_metadata):
    """
    Gets a list of all storage connectors and their type in a featurestore

    Args:
        :featurestore_metadata: metadata of the featurestore

    Returns:
        A list of names of the storage connectors in this featurestore and their type
    """
    return list(map(lambda sc: (sc.name, sc.type), featurestore_metadata.storage_connectors.values()))


def _do_get_training_dataset_path(training_dataset_name, featurestore_metadata, training_dataset_version=1):
    """
    Gets the HDFS path to a training dataset with a specific name and version in a featurestore

    Args:
        :training_dataset_name: name of the training dataset
        :featurestore_metadata: metadata of the featurestore
        :training_dataset_version: version of the training dataset

    Returns:
        The HDFS path to the training dataset
    """
    training_dataset = query_planner._find_training_dataset(featurestore_metadata.training_datasets,
                                                            training_dataset_name,
                                                            training_dataset_version)
    hdfs_path = training_dataset.hopsfs_training_dataset.hdfs_store_path + \
        constants.DELIMITERS.SLASH_DELIMITER + training_dataset.name
    data_format = training_dataset.data_format
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_NPY_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_NPY_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_HDF5_FORMAT:
        hdfs_path = hdfs_path + constants.FEATURE_STORE.TRAINING_DATASET_HDF5_SUFFIX
    if data_format == constants.FEATURE_STORE.TRAINING_DATASET_IMAGE_FORMAT:
        hdfs_path = training_dataset.hopsfs_training_dataset.hdfs_store_path
    # abspath means "hdfs://namenode:port/ is preprended
    abspath = util.abspath(hdfs_path)
    return abspath


def _do_get_featuregroup_partitions(featuregroup_name, featurestore_metadata, featurestore=None, featuregroup_version=1,
                                    online=False):
    """
    Gets the partitions of a featuregroup

     Args:
        :featuregroup_name: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :online: a boolean flag whether to fetch the online feature or the offline one (assuming that the
                 feature group that the feature is stored in has online serving enabled)
                 (for cached feature groups only)

     Returns:
        a dataframe with the partitions of the featuregroup
     """
    fg = query_planner._find_featuregroup(
        featurestore_metadata.featuregroups, featuregroup_name, featuregroup_version)
    if fg.featuregroup_type == featurestore_metadata.settings.on_demand_featuregroup_type:
        raise CannotGetPartitionsOfOnDemandFeatureGroup("The feature group with name: {} , and version: {} "
                                                        "is an on-demand feature group. "
                                                        "Get partitions operation is only supported for "
                                                        "cached feature groups."
                                                        .format(featuregroup_name, featuregroup_version))

    sql_str = "SHOW PARTITIONS " + \
        fs_utils._get_table_name(featuregroup_name, featuregroup_version)
    result = _run_and_log_sql(sql_str, featurestore, online)
    return result


def _do_visualize_featuregroup_distributions(featuregroup_name, featurestore=None, featuregroup_version=1,
                                             figsize=(16, 12), color='lightblue', log=False, align="center"):
    """
    Creates a matplotlib figure of the feature distributions in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature distributions have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.

    Returns:
        Matplotlib figure with the feature distributions

    Raises:
        :FeatureDistributionsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.feature_histograms is None or stats.feature_histograms.feature_distributions is None:
        raise FeatureDistributionsNotComputed("Cannot visualize the feature distributions for the "
                                              "feature group: {} with version: {} in featurestore: {} since the "
                                              "feature distributions have not been computed for this featuregroup."
                                              " To compute the feature distributions, call "
                                              "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_distributions(stats.feature_histograms.feature_distributions,
                                                            figsize=figsize, color=color, log=log, align=align)
    return fig


def _do_visualize_featuregroup_correlations(featuregroup_name, featurestore=None, featuregroup_version=1,
                                            figsize=(16, 12), cmap="coolwarm", annot=True, fmt=".2f", linewidths=.05):
    """
    Creates a matplotlib figure of the feature correlations in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature correlations have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        Matplotlib figure with the feature correlations

    Raises:
        :FeatureCorrelationsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.correlation_matrix is None or stats.correlation_matrix.feature_correlations is None:
        raise FeatureCorrelationsNotComputed("Cannot visualize the feature correlations for the "
                                             "feature group: {} with version: {} in featurestore: {} since the "
                                             "feature correlations have not been computed for this featuregroup."
                                             " To compute the feature correlations, call "
                                             "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_correlations(stats.correlation_matrix.feature_correlations,
                                                           figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                           linewidths=linewidths)
    return fig


def _do_visualize_featuregroup_clusters(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16, 12)):
    """
    Creates a matplotlib figure of the feature clusters in a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the feature clusters have been computed for the featuregroup, create the figure

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup
        :figsize: the size of the figure

    Returns:
        Matplotlib figure with the feature clusters

    Raises:
        :FeatureClustersNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.cluster_analysis is None:
        raise FeatureClustersNotComputed("Cannot visualize the feature clusters for the "
                                         "feature group: {} with version: {} in featurestore: {} since the "
                                         "feature clusters have not been computed for this featuregroup."
                                         " To compute the feature clusters, call "
                                         "featurestore.update_featuregroup_stats(featuregroup_name)")
    fig = statistics_plots._visualize_feature_clusters(
        stats.cluster_analysis, figsize=figsize)
    return fig


def _do_visualize_featuregroup_descriptive_stats(featuregroup_name, featurestore=None,
                                                 featuregroup_version=1):
    """
    Creates a pandas dataframe of the descriptive statistics of a featuregroup in the featurestore.

    1. Fetches the stored statistics for the featuregroup
    2. If the descriptive statistics have been computed for the featuregroup, create the pandas dataframe

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
        Pandas dataframe with the descriptive statistics

    Raises:
        :DescriptiveStatisticsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_featuregroup_statistics(featuregroup_name, featurestore=featurestore,
                                            featuregroup_version=featuregroup_version)
    if stats.descriptive_stats is None or stats.descriptive_stats.descriptive_stats is None:
        raise DescriptiveStatisticsNotComputed("Cannot visualize the descriptive statistics for the "
                                               "featuregroup: {} with version: {} in featurestore: {} since the "
                                               "descriptive statistics have not been computed for this featuregroup."
                                               " To compute the descriptive statistics, call "
                                               "featurestore.update_featuregroup_stats(featuregroup_name)")
    df = statistics_plots._visualize_descriptive_stats(
        stats.descriptive_stats.descriptive_stats)
    return df


def _do_visualize_training_dataset_distributions(training_dataset_name, featurestore=None, training_dataset_version=1,
                                                 figsize=(16, 12), color='lightblue', log=False, align="center"):
    """
    Creates a matplotlib figure of the feature distributions in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature distributions have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: size of the figure
        :color: the color of the histograms
        :log: whether to use log-scaling on the y-axis or not
        :align: how to align the bars, defaults to center.

    Returns:
        Matplotlib figure with the feature distributions

    Raises:
        :FeatureDistributionsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.feature_histograms is None or stats.feature_histograms.feature_distributions is None:
        raise FeatureDistributionsNotComputed("Cannot visualize the feature distributions for the "
                                              "training dataset: {} with version: {} in featurestore: {} since the "
                                              "feature distributions have not been computed for this training dataset."
                                              " To compute the feature distributions, call "
                                              "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_distributions(stats.feature_histograms.feature_distributions,
                                                            figsize=figsize, color=color, log=log, align=align)
    return fig


def _do_visualize_training_dataset_correlations(training_dataset_name, featurestore=None, training_dataset_version=1,
                                                figsize=(16, 12), cmap="coolwarm", annot=True, fmt=".2f",
                                                linewidths=.05):
    """
    Creates a matplotlib figure of the feature correlations in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature correlations have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :tranining_dataset_version: the version of the training dataset
        :figsize: the size of the figure
        :cmap: the color map
        :annot: whether to annotate the heatmap
        :fmt: how to format the annotations
        :linewidths: line width in the plot

    Returns:
        Matplotlib figure with the feature correlations

    Raises:
        :FeatureCorrelationsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.correlation_matrix is None or stats.correlation_matrix.feature_correlations is None:
        raise FeatureCorrelationsNotComputed("Cannot visualize the feature correlations for the "
                                             "training dataset: {} with version: {} in featurestore: {} since the "
                                             "feature correlations have not been computed for this training dataset."
                                             " To compute the feature correlations, call "
                                             "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_correlations(stats.correlation_matrix.feature_correlations,
                                                           figsize=figsize, cmap=cmap, annot=annot, fmt=fmt,
                                                           linewidths=linewidths)
    return fig


def _do_visualize_training_dataset_clusters(training_dataset_name, featurestore=None, training_dataset_version=1,
                                            figsize=(16, 12)):
    """
    Creates a matplotlib figure of the feature clusters in a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the feature clusters have been computed for the training dataset, create the figure

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :figsize: the size of the figure

    Returns:
        Matplotlib figure with the feature clusters

    Raises:
        :FeatureClustersNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.cluster_analysis is None:
        raise FeatureClustersNotComputed("Cannot visualize the feature clusters for the "
                                         "training dataset: {} with version: {} in featurestore: {} since the "
                                         "feature clusters have not been computed for this training dataset."
                                         " To compute the feature clusters, call "
                                         "featurestore.update_training_dataset_stats(training_dataset_name)")
    fig = statistics_plots._visualize_feature_clusters(
        stats.cluster_analysis, figsize=figsize)
    return fig


def _do_visualize_training_dataset_descriptive_stats(training_dataset_name, featurestore=None,
                                                     training_dataset_version=1):
    """
    Creates a pandas dataframe of the descriptive statistics of a training dataset in the featurestore.

    1. Fetches the stored statistics for the training dataset
    2. If the descriptive statistics have been computed for the training dataset, create the pandas dataframe

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
        Pandas dataframe with the descriptive statistics

    Raises:
        :DescriptiveStatisticsNotComputed: if the feature distributions to visualize have not been computed.
    """
    stats = _do_get_training_dataset_statistics(training_dataset_name, featurestore=featurestore,
                                                training_dataset_version=training_dataset_version)
    if stats.descriptive_stats is None or stats.descriptive_stats.descriptive_stats is None:
        raise DescriptiveStatisticsNotComputed("Cannot visualize the descriptive statistics for the "
                                               "training dataset: {} with version: {} in featurestore: {} since the "
                                               "descriptive statistics have not been computed for this training dataset."
                                               " To compute the descriptive statistics, call "
                                               "featurestore.update_training_dataset_stats(training_dataset_name)")
    df = statistics_plots._visualize_descriptive_stats(
        stats.descriptive_stats.descriptive_stats)
    return df


def _do_get_featuregroup_statistics(featuregroup_name, featurestore=None, featuregroup_version=1):
    """
    Gets the computed statistics (if any) of a featuregroup

    Args:
        :featuregroup_name: the name of the featuregroup
        :featurestore: the featurestore where the featuregroup resides
        :featuregroup_version: the version of the featuregroup

    Returns:
          A Statistics Object
    """
    featuregroup_id = _get_featuregroup_id(
        featurestore, featuregroup_name, featuregroup_version)
    featurestore_id = _get_featurestore_id(featurestore)
    response_object = rest_rpc._get_featuregroup_rest(
        featuregroup_id, featurestore_id)
    # .get() returns None if key dont exists intead of exception
    descriptive_stats_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS)
    correlation_matrix_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION)
    features_histogram_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM)
    feature_clusters = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS)
    return Statistics(descriptive_stats_json, correlation_matrix_json, features_histogram_json, feature_clusters)


def _do_get_training_dataset_statistics(training_dataset_name, featurestore=None, training_dataset_version=1):
    """
    Gets the computed statistics (if any) of a training dataset

    Args:
        :training_dataset_name: the name of the training dataset
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset

    Returns:
          A Statistics Object
    """
    training_dataset_id = _get_training_dataset_id(
        featurestore, training_dataset_name, training_dataset_version)
    featurestore_id = _get_featurestore_id(featurestore)
    response_object = rest_rpc._get_training_dataset_rest(
        training_dataset_id, featurestore_id)
    # .get() returns None if key dont exists intead of exception
    descriptive_stats_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_DESC_STATS)
    correlation_matrix_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURE_CORRELATION)
    features_histogram_json = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_HISTOGRAM)
    feature_clusters = response_object.get(
        constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES_CLUSTERS)
    return Statistics(descriptive_stats_json, correlation_matrix_json, features_histogram_json, feature_clusters)


def _do_get_online_featurestore_connector(featurestore, featurestore_metadata):
    """
    Gets the JDBC connector for the online featurestore
    Args:
        :featurestore: the featurestore name
        :featurestore_metadata: the featurestore metadata
    Returns:
        a JDBC connector DTO object for the online featurestore
    """
    featurestore_id = _get_featurestore_id(featurestore)

    if featurestore_metadata is not None and featurestore_metadata.online_featurestore_connector is not None:
        return featurestore_metadata.online_featurestore_connector
    else:
        response_object = rest_rpc._get_online_featurestore_jdbc_connector_rest(
            featurestore_id)
        return JDBCStorageConnector(response_object)


def _do_import_featuregroup(job_conf):
    return rest_rpc._put_featuregroup_import_job(job_conf)


def _do_trainingdataset_create(job_conf):
    """
    Creates a job with `job_conf` through a REST call to create a training
    dataset.

    Args:
        :job_conf: training dataset creation job configuration

    Returns:
        The REST response

    Raises:
        :RestAPIError: if there was an error in the REST call to Hopsworks
    """
    return rest_rpc._put_trainingdataset_create_job(job_conf)


# Fetch on-load and cache it on the client
try:
    metadata_cache = _get_featurestore_metadata(
        featurestore=fs_utils._do_get_project_featurestore())
except:
    pass
