"""
A feature store client. This module exposes an API for interacting with feature stores in Hopsworks.
It hides complexity and provides utility methods such as:

    - `project_featurestore()`.
    - `get_featuregroup()`.
    - `get_feature()`.
    - `get_features()`.
    - `sql()`
    - `insert_into_featuregroup()`
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
    >>> # Insert into featuregroup example
    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode
    >>> # (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features",
    >>>                                      featurestore=featurestore.project_featurestore(),
    >>>                                      featuregroup_version=1, mode="append", descriptive_statistics=True,
    >>>                                      feature_correlation=True, feature_histograms=True, cluster_analysis=True,
    >>>                                      stat_columns=None)
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
    >>> # Compute featuergroup statistics (feature correlation, descriptive stats, feature distributions etc)
    >>> # with Spark that will show up in the Featurestore Registry in Hopsworks
    >>> # The API will default to the project's featurestore, featuregroup version 1, and
    >>> # compute all statistics for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True,feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns
    >>> # for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True, feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True,
    >>>                                        stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])
    >>>
    >>> # Create featuregroup from an existing dataframe
    >>> # In most cases it is recommended that featuregroups are created in the UI on Hopsworks and that care is
    >>> # taken in documenting the featuregroup.
    >>> # However, sometimes it is practical to create a featuregroup directly from a spark dataframe and
    >>> # fill in the metadata about the featuregroup later in the UI.
    >>> # This can be done through the create_featuregroup API function
    >>>
    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for
    >>> # the new featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2",
    >>>                                  description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2",
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None)
    >>>
    >>> # After you have found the features you need in the featurestore you can materialize the features into a
    >>> # training dataset so that you can train a machine learning model using the features. Just as for featuregroups,
    >>> # it is useful to version and document training datasets, for this reason HopsML supports managed training
    >>> # datasets which enables you to easily version, document and automate the materialization of training datasets.
    >>>
    >>> # First we select the features (and/or labels) that we want
    >>> dataset_df = featurestore.get_features(["pagerank", "triangle_count", "avg_trx", "count_trx", "max_trx",
    >>>                                         "min_trx","balance", "number_of_accounts"],
    >>>                                        featurestore=featurestore.project_featurestore())
    >>> # Now we can create a training dataset from the dataframe with some extended metadata such as schema
    >>> # (automatically inferred).
    >>> # By default when you create a training dataset it will be in "tfrecords" format and statistics will be
    >>> # computed for all features.
    >>> # After the dataset have been created you can view and/or update the metadata about the training dataset
    >>> # from the Hopsworks featurestore UI
    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="",
    >>>                                      featurestore=featurestore.project_featurestore(), data_format="csv",
    >>>                                      training_dataset_version=1, jobs=[],
    >>>                                      descriptive_statistics=False, feature_correlation=False,
    >>>                                      feature_histograms=False, cluster_analysis=False, stat_columns=None)
    >>>
    >>> # Once a dataset have been created, its metadata is browsable in the featurestore registry
    >>> # in the Hopsworks UI.
    >>> # If you don't want to create a new training dataset but just overwrite or insert new data into an
    >>> # existing training dataset,
    >>> # you can use the API function 'insert_into_training_dataset'
    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset",
    >>>                                           featurestore=featurestore.project_featurestore(),
    >>>                                           training_dataset_version=1, descriptive_statistics=True,
    >>>                                           feature_correlation=True, feature_histograms=True,
    >>>                                           cluster_analysis=True, stat_columns=None)
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

from hops import util, constants
from hops.featurestore_impl.rest import rest_rpc
from hops.featurestore_impl.util import fs_utils
from hops.featurestore_impl import core
from hops.featurestore_impl.exceptions.exceptions import CouldNotConvertDataframe, FeatureVisualizationError, \
    StatisticsComputationError
import os
from pyhive import hive
import pandas as pd


def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    return fs_utils._do_get_project_featurestore()


def project_training_datasets_sink():
    """
    Gets the project's training datasets sink

    Returns:
        the default training datasets folder in HopsFS for the project

    """
    return fs_utils._do_get_project_training_datasets_sink()


def get_featuregroup(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="pandas", jdbc_args={}):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

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
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to be filled in
                    dynamically at runtime for fetching on-demand feature groups

    Returns:
        a dataframe with the contents of the featuregroup

    """
    if featurestore is None:
        featurestore = project_featurestore()

    try:  # Try with cached metadata
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore, update_cache=False),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         dataframe_type=dataframe_type, jdbc_args=jdbc_args)
    except:  # Try again after updating the cache
        return core._do_get_featuregroup(featuregroup,
                                         core._get_featurestore_metadata(featurestore, update_cache=True),
                                         featurestore=featurestore, featuregroup_version=featuregroup_version,
                                         dataframe_type=dataframe_type, jdbc_args=jdbc_args)


def get_feature(feature, featurestore=None, featuregroup=None, featuregroup_version=1, dataframe_type="spark",
                jdbc_args={}):
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
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of argument_name -> value with jdbc connection string arguments to
                    be filled in dynamically at runtime for fetching on-demand feature group in-case the feature
                    belongs to a dynamic feature group

    Returns:
        A dataframe with the feature

    """
    try:  # try with cached metadata
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=False),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, dataframe_type=dataframe_type,
                                    jdbc_args=jdbc_args)
    except:  # Try again after updating cache
        return core._do_get_feature(feature, core._get_featurestore_metadata(featurestore, update_cache=True),
                                    featurestore=featurestore, featuregroup=featuregroup,
                                    featuregroup_version=featuregroup_version, dataframe_type=dataframe_type,
                                    jdbc_args=jdbc_args)


def get_features(features, featurestore=None, featuregroups_version_dict={}, join_key=None, dataframe_type="spark",
                 jdbc_args = {}):
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
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)
        :jdbc_args: a dict of featuregroup_version -> dict of argument_name -> value with jdbc connection string arguments to
                    be filled in dynamically at runtime for fetching on-demand feature groups

    Returns:
        A dataframe with all the features

    """
    # try with cached metadata
    try:
        return core._do_get_features(features,
                                     core._get_featurestore_metadata(featurestore, update_cache=False),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key, dataframe_type=dataframe_type, jdbc_args=jdbc_args)
        # Try again after updating cache
    except:
        return core._do_get_features(features, core._get_featurestore_metadata(featurestore, update_cache=True),
                                     featurestore=featurestore,
                                     featuregroups_version_dict=featuregroups_version_dict,
                                     join_key=join_key, dataframe_type=dataframe_type, jdbc_args=jdbc_args)


def sql(query, featurestore=None):
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
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        (pandas.DataFrame): A pandas dataframe with the query results
    """
    if featurestore is None:
        featurestore = project_featurestore()

    hive_conn = util._create_hive_connection(featurestore)
    dataframe = core._run_and_log_sql(hive_conn, query)

    return dataframe

    ### old spark code
    # spark = util._find_spark()
    # core._verify_hive_enabled(spark)
    # spark.sparkContext.setJobGroup("Running SQL query against feature store",
    #                                "Running query: {} on the featurestore {}".format(query, featurestore))
    # core._use_featurestore(spark, featurestore)
    # result = core._run_and_log_sql(spark, query)
    # spark.sparkContext.setJobGroup("", "")
    # return fs_utils._return_dataframe_type(result, dataframe_type)


def insert_into_featuregroup(df, featuregroup, featurestore=None, featuregroup_version=1, mode="append",
                             descriptive_statistics=True, feature_correlation=True, feature_histograms=True,
                             cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
                             num_clusters=5):
    """
    Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
    This will append to  the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
    from the UI and append to that table.

    Example usage:

    >>> # The API will default to the project's feature store, featuegroup version 1, and write mode 'append'
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features")
    >>> # You can also explicitly define the feature store, the featuregroup version, and the write mode
    >>> # (only append and overwrite are supported)
    >>> featurestore.insert_into_featuregroup(sampleDf, "trx_graph_summary_features",
    >>>                                      featurestore=featurestore.project_featurestore(), featuregroup_version=1,
    >>>                                      mode="append", descriptive_statistics=True, feature_correlation=True,
    >>>                                      feature_histograms=True, cluster_analysis=True,
    >>>                                      stat_columns=None)

    Args:
        :df: the dataframe containing the data to insert into the featuregroup
        :featuregroup: the name of the featuregroup (hive table name)
        :featurestore: the featurestore to save the featuregroup to (hive database)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :mode: the write mode, only 'overwrite' and 'append' are supported
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                          featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    try:
        # Try with cached metadata
        core._do_insert_into_featuregroup(df, featuregroup,
                                          core._get_featurestore_metadata(featurestore, update_cache=False),
                                          featurestore=featurestore, featuregroup_version=featuregroup_version,
                                          mode=mode, descriptive_statistics=descriptive_statistics,
                                          feature_correlation=feature_correlation,
                                          feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                          stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                                          num_clusters=num_clusters)
    except:
        # Retry with updated cache
        core._do_insert_into_featuregroup(df, featuregroup,
                                          core._get_featurestore_metadata(featurestore, update_cache=True),
                                          featurestore=featurestore, featuregroup_version=featuregroup_version,
                                          mode=mode, descriptive_statistics=descriptive_statistics,
                                          feature_correlation=feature_correlation,
                                          feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                          stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                                          num_clusters=num_clusters)


def update_featuregroup_stats(featuregroup, featuregroup_version=1, featurestore=None, descriptive_statistics=True,
                              feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                              stat_columns=None, num_bins=20, num_clusters=5, corr_method='pearson'):
    """
    Updates the statistics of a featuregroup by computing the statistics with spark and then saving it to Hopsworks by
    making a REST call.

    Example usage:

    >>> # The API will default to the project's featurestore, featuregroup version 1, and compute all statistics
    >>> # for all columns
    >>> featurestore.update_featuregroup_stats("trx_summary_features")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                       featurestore=featurestore.project_featurestore(),
    >>>                                       descriptive_statistics=True,feature_correlation=True,
    >>>                                       feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns for
    >>> # example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_featuregroup_stats("trx_summary_features", featuregroup_version=1,
    >>>                                        featurestore=featurestore.project_featurestore(),
    >>>                                        descriptive_statistics=True, feature_correlation=True,
    >>>                                        feature_histograms=True, cluster_analysis=True,
    >>>                                        stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :featuregroup: the featuregroup to update the statistics for
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :featurestore: the featurestore where the featuregroup resides (defaults to the project's featurestore)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                 for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use in clustering analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    try:
        core._do_update_featuregroup_stats(featuregroup,
                                           core._get_featurestore_metadata(featurestore, update_cache=False),
                                           featuregroup_version=featuregroup_version, featurestore=featurestore,
                                           descriptive_statistics=descriptive_statistics,
                                           feature_correlation=feature_correlation,
                                           feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                           stat_columns=stat_columns, num_bins=num_bins, num_clusters=num_clusters,
                                           corr_method=corr_method)
    except:
        # Retry with updated cache
        try:
            core._do_update_featuregroup_stats(featuregroup,
                                               core._get_featurestore_metadata(featurestore, update_cache=True),
                                               featuregroup_version=featuregroup_version, featurestore=featurestore,
                                               descriptive_statistics=descriptive_statistics,
                                               feature_correlation=feature_correlation,
                                               feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                               stat_columns=stat_columns, num_bins=num_bins, num_clusters=num_clusters,
                                               corr_method=corr_method)
        except Exception as e:
            raise StatisticsComputationError("There was an error in computing the statistics for feature group: {}"
                                             " , with version: {} in featurestore: {}. "
                                             "Error: {}".format(featuregroup, featuregroup_version,
                                                                featurestore, str(e)))


def create_on_demand_featuregroup(sql_query, featuregroup, jdbc_connector_name, featurestore=None,
                                  description="", featuregroup_version=1):
    """
    Creates a new on-demand feature group in the feature store by registering SQL and an associated JDBC connector

    Args:
        :sql_query: the SQL query to fetch the on-demand feature group
        :featuregroup: the name of the on-demand feature group
        :jdbc_connector_name: the name of the JDBC connector to apply the SQL query to get the on-demand feature group
        :featurestore: name of the feature store to register the feature group
        :description: description of the feature group
        :featuregroup_version: version of the feature group

    Returns:
        None

    Raises:
        :ValueError: in case required inputs are missing
    """
    if featurestore is None:
        featurestore = project_featurestore()
    if sql_query is None:
        raise ValueError("SQL Query for an on-demand Feature Group cannot be None")
    if jdbc_connector_name is None:
        raise ValueError("Storage Connector for an on-demand Feature Group cannot be None")
    jdbc_connector = get_storage_connector(jdbc_connector_name, featurestore)
    featurestore_metadata = core._get_featurestore_metadata(featurestore, update_cache=False)
    if jdbc_connector.type != featurestore_metadata.settings.jdbc_connector_type:
        raise ValueError("OnDemand Feature groups can only be linked to JDBC Storage Connectors, the provided "
                         "connector is of type: {}".format(jdbc_connector.type))
    featurestore_id = core._get_featurestore_id(featurestore)
    featuregroup_type, featuregroup_type_dto = fs_utils._get_on_demand_featuregroup_type_info(featurestore_metadata)
    rest_rpc._create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, [],
                                       None, None, None, None, None, featuregroup_type, featuregroup_type_dto,
                                       sql_query, jdbc_connector.id)
    # update metadata cache
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    fs_utils._log("Feature group created successfully")


def create_featuregroup(df, featuregroup, primary_key=None, description="", featurestore=None,
                        featuregroup_version=1, jobs=[],
                        descriptive_statistics=True, feature_correlation=True,
                        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                        corr_method='pearson', num_clusters=5, partition_by=[]):
    """
    Creates a new cached featuregroup from a dataframe of features (sends the metadata to Hopsworks with a REST call
    to create the Hive table and store the metadata and then inserts the data of the spark dataframe into the newly
    created table)

    Example usage:

    >>> # By default the new featuregroup will be created in the project's featurestore and the statistics for the new
    >>> # featuregroup will be computed based on the provided spark dataframe.
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2",
    >>>                                  description="trx_summary_features without the column count_trx")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.create_featuregroup(trx_summary_df1, "trx_summary_features_2_2",
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[])

    Args:
        :df: the dataframe to create the featuregroup for (used to infer the schema)
        :featuregroup: the name of the new featuregroup
        :primary_key: the primary key of the new featuregroup, if not specified, the first column in the dataframe will
                      be used as primary
        :description: a description of the featuregroup
        :featurestore: the featurestore of the featuregroup (defaults to the project's featurestore)
        :featuregroup_version: the version of the featuregroup (defaults to 1)
        :jobs: list of Hopsworks jobs linked to the feature group
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for the
                                 featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns in
                              the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :partition_by: a list of columns to partition_by, defaults to the empty list

    Returns:
        None

    Raises:
        :CouldNotConvertDataframe: in case the provided dataframe could not be converted to a spark dataframe
    """
    try:
        spark_df = fs_utils._convert_dataframe_to_spark(df)
    except Exception as e:
        raise CouldNotConvertDataframe(
            "Could not convert the provided dataframe to a spark dataframe which is required in order to save it to "
            "the Feature Store, error: {}".format(
                str(e)))

    fs_utils._validate_metadata(featuregroup, spark_df.dtypes, description)

    if featurestore is None:
        featurestore = project_featurestore()
    if primary_key is None:
        primary_key = fs_utils._get_default_primary_key(spark_df)
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())

    fs_utils._validate_primary_key(spark_df, primary_key)
    features_schema = core._parse_spark_features_schema(spark_df.schema, primary_key, partition_by)
    feature_corr_data, featuregroup_desc_stats_data, features_histogram_data, cluster_analysis_data = \
        core._compute_dataframe_stats(
            spark_df, featuregroup, version=featuregroup_version,
            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis, stat_columns=stat_columns,
            num_bins=num_bins,
            corr_method=corr_method,
            num_clusters=num_clusters)
    featurestore_metadata = core._get_featurestore_metadata(featurestore, update_cache=False)
    featurestore_id = core._get_featurestore_id(featurestore)
    featuregroup_type, featuregroup_type_dto = fs_utils._get_cached_featuregroup_type_info(featurestore_metadata)
    rest_rpc._create_featuregroup_rest(featuregroup, featurestore_id, description, featuregroup_version, jobs,
                                       features_schema, feature_corr_data, featuregroup_desc_stats_data,
                                       features_histogram_data, cluster_analysis_data, featuregroup_type,
                                       featuregroup_type_dto, None, None)
    core._write_featuregroup_hive(spark_df, featuregroup, featurestore, featuregroup_version,
                                  constants.FEATURE_STORE.FEATURE_GROUP_INSERT_APPEND_MODE)
    # update metadata cache
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass
    fs_utils._log("Feature group created successfully")


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

    Returns:
        A list of featuregroups and their metadata

    """
    if featurestore is None:
        featurestore = project_featurestore()
    return core._get_featurestore_metadata(featurestore=featurestore, update_cache=update_cache)


def get_featuregroups(featurestore=None):
    """
    Gets a list of all featuregroups in a featurestore, uses the cached metadata.

    >>> # List all Feature Groups in a Feature Store
    >>> featurestore.get_featuregroups()
    >>> # By default `get_featuregroups()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_featuregroups(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list featuregroups for, defaults to the project-featurestore

    Returns:
        A list of names of the featuregroups in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()

    # Try with the cache first
    try:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=False))
    # If it fails, update cache
    except:
        return fs_utils._do_get_featuregroups(core._get_featurestore_metadata(featurestore, update_cache=True))


def get_features_list(featurestore=None):
    """
    Gets a list of all features in a featurestore, will use the cached featurestore metadata

    >>> # List all Features in a Feature Store
    >>> featurestore.get_features_list()
    >>> # By default `get_features_list()` will use the project's feature store, but this can also be specified
    >>> # with the optional argument `featurestore`
    >>> featurestore.get_features_list(featurestore=featurestore.project_featurestore())

    Args:
        :featurestore: the featurestore to list features for, defaults to the project-featurestore

    Returns:
        A list of names of the features in this featurestore
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=False))
    except:
        return fs_utils._do_get_features_list(core._get_featurestore_metadata(featurestore, update_cache=True))


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


def get_dataframe_tf_record_schema(spark_df, fixed=True):
    """
    Infers the tf-record schema from a spark dataframe
    Note: this method is just for convenience, it should work in 99% of cases but it is not guaranteed,
    if spark or tensorflow introduces new datatypes this will break. The user can allways fallback to encoding the
    tf-example-schema manually.

    Args:
        :spark_df: the spark dataframe to infer the tensorflow example record from
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size

    Returns:
        a dict with the tensorflow example
    """
    return fs_utils._get_dataframe_tf_record_schema_json(spark_df, fixed=fixed)[0]


def get_training_dataset_tf_record_schema(training_dataset, training_dataset_version=1, featurestore=None):
    """
    Gets the tf record schema for a training dataset that is stored in tfrecords format

    Example usage:

    >>> # get tf record schema for a tfrecords dataset
    >>> featurestore.get_training_dataset_tf_record_schema("team_position_prediction", training_dataset_version=1,
    >>>                                                    featurestore = featurestore.project_featurestore())

    Args:
        :training_dataset: the training dataset to get the tfrecords schema for
        :training_dataset_version: the version of the training dataset
        :featurestore: the feature store where the training dataset resides

    Returns:
        the tf records schema

    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset_tf_record_schema(training_dataset,
                                                              core._get_featurestore_metadata(featurestore,
                                                                                              update_cache=False),
                                                              training_dataset_version=training_dataset_version,
                                                              featurestore=featurestore)
    except:
        return core._do_get_training_dataset_tf_record_schema(training_dataset,
                                                              core._get_featurestore_metadata(featurestore,
                                                                                              update_cache=True),
                                                              training_dataset_version=training_dataset_version,
                                                              featurestore=featurestore)


def get_training_dataset(training_dataset, featurestore=None, training_dataset_version=1, dataframe_type="spark"):
    """
    Reads a training dataset into a spark dataframe, will first look for the training dataset using the cached metadata
    of the featurestore, if it fails it will reload the metadata and try again.

    Example usage:
    >>> featurestore.get_training_dataset("team_position_prediction_csv").show(5)

    Args:
        :training_dataset: the name of the training dataset to read
        :featurestore: the featurestore where the training dataset resides
        :training_dataset_version: the version of the training dataset
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

    Returns:
        A dataframe with the given training dataset data
    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        return core._do_get_training_dataset(training_dataset,
                                             core._get_featurestore_metadata(featurestore, update_cache=False),
                                             training_dataset_version=training_dataset_version,
                                             dataframe_type=dataframe_type,
                                             featurestore=featurestore)
    except:
        return core._do_get_training_dataset(training_dataset,
                                             core._get_featurestore_metadata(featurestore, update_cache=True),
                                             training_dataset_version=training_dataset_version,
                                             dataframe_type=dataframe_type)


def create_training_dataset(df, training_dataset, description="", featurestore=None,
                            data_format="tfrecords", training_dataset_version=1,
                            jobs=[], descriptive_statistics=True, feature_correlation=True,
                            feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                            corr_method='pearson', num_clusters=5, petastorm_args={}, fixed=True, sink=None):
    """
    Creates a new training dataset from a dataframe, saves metadata about the training dataset to the database
    and saves the materialized dataset on hdfs

    Example usage:

    >>> featurestore.create_training_dataset(dataset_df, "AML_dataset")
    >>> # You can override the default configuration if necessary:
    >>> featurestore.create_training_dataset(dataset_df, "TestDataset", description="",
    >>>                                      featurestore=featurestore.project_featurestore(), data_format="csv",
    >>>                                      training_dataset_version=1,
    >>>                                      descriptive_statistics=False, feature_correlation=False,
    >>>                                      feature_histograms=False, cluster_analysis=False, stat_columns=None,
    >>>                                      sink = None)

    Args:
        :df: the dataframe to create the training dataset from
        :training_dataset: the name of the training dataset
        :description: a description of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :data_format: the format of the materialized training dataset
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in the
                           featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :petastorm_args: a dict containing petastorm parameters for serializing a dataset in the
                         petastorm format. Required parameters are: 'schema'
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size
        :sink: name of storage connector to store the training dataset
        :jobs: list of Hopsworks jobs linked to the training dataset

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()
    if sink is None:
        sink = project_training_datasets_sink()
    if util.get_job_name() is not None:
        jobs.append(util.get_job_name())
    storage_connector = core._do_get_storage_connector(sink, featurestore)
    core._do_create_training_dataset(df, training_dataset, description, featurestore, data_format,
                                     training_dataset_version, jobs, descriptive_statistics,
                                     feature_correlation, feature_histograms, cluster_analysis, stat_columns,
                                     num_bins, corr_method, num_clusters, petastorm_args, fixed, storage_connector)


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


def insert_into_training_dataset(
        df, training_dataset, featurestore=None, training_dataset_version=1,
        descriptive_statistics=True, feature_correlation=True,
        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20, corr_method='pearson',
        num_clusters=5, write_mode="overwrite", ):
    """
    Inserts the data in a training dataset from a spark dataframe (append or overwrite)

    Example usage:

    >>> featurestore.insert_into_training_dataset(dataset_df, "TestDataset")
    >>> # By default the insert_into_training_dataset will use the project's featurestore, version 1,
    >>> # and update the training dataset statistics, this configuration can be overridden:
    >>> featurestore.insert_into_training_dataset(dataset_df,"TestDataset",
    >>>                                           featurestore=featurestore.project_featurestore(),
    >>>                                           training_dataset_version=1,descriptive_statistics=True,
    >>>                                           feature_correlation=True, feature_histograms=True,
    >>>                                           cluster_analysis=True, stat_columns=None)

    Args:
        :df: the dataframe to write
        :training_dataset: the name of the training dataset
        :featurestore: the featurestore that the training dataset is linked to
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc)
                                for the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns
                          in the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: number of clusters to use for cluster analysis
        :corr_method: the method to compute feature correlation with (pearson or spearman)
        :write_mode: spark write mode ('append' or 'overwrite'). Note: append is not supported for tfrecords datasets.

    Returns:
        None

    """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        core._do_insert_into_training_dataset(df, training_dataset,
                                              core._get_featurestore_metadata(featurestore,
                                                                              update_cache=False),
                                              featurestore,
                                              training_dataset_version=training_dataset_version,
                                              descriptive_statistics=descriptive_statistics,
                                              feature_correlation=feature_correlation,
                                              feature_histograms=feature_histograms,
                                              cluster_analysis=cluster_analysis, stat_columns=stat_columns,
                                              num_bins=num_bins,
                                              corr_method=corr_method, num_clusters=num_clusters,
                                              write_mode=write_mode)
        fs_utils._log("Insertion into training dataset was successful")
    except:
        core._do_insert_into_training_dataset(df, training_dataset,
                                              core._get_featurestore_metadata(featurestore,
                                                                              update_cache=True),
                                              featurestore,
                                              training_dataset_version=training_dataset_version,
                                              descriptive_statistics=descriptive_statistics,
                                              feature_correlation=feature_correlation,
                                              feature_histograms=feature_histograms,
                                              cluster_analysis=cluster_analysis, stat_columns=stat_columns,
                                              num_bins=num_bins,
                                              corr_method=corr_method, num_clusters=num_clusters,
                                              write_mode=write_mode)
        fs_utils._log("Insertion into training dataset was successful")


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


def update_training_dataset_stats(training_dataset, training_dataset_version=1, featurestore=None,
                                  descriptive_statistics=True,
                                  feature_correlation=True, feature_histograms=True, cluster_analysis=True,
                                  stat_columns=None, num_bins=20,
                                  num_clusters=5, corr_method='pearson'):
    """
    Updates the statistics of a featuregroup by computing the statistics with spark and then saving it to Hopsworks by
    making a REST call.

    Example usage:

    >>> # The API will default to the project's featurestore, training dataset version 1, and compute all statistics
    >>> # for all columns
    >>> featurestore.update_training_dataset_stats("teams_prediction")
    >>> # You can also be explicitly specify featuregroup details and what statistics to compute:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1,
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            descriptive_statistics=True,feature_correlation=True,
    >>>                                            feature_histograms=True, cluster_analysis=True, stat_columns=None)
    >>> # If you only want to compute statistics for certain set of columns and exclude surrogate key-columns
    >>> # for example, you can use the optional argument stat_columns to specify which columns to include:
    >>> featurestore.update_training_dataset_stats("teams_prediction", training_dataset_version=1,
    >>>                                            featurestore=featurestore.project_featurestore(),
    >>>                                            descriptive_statistics=True, feature_correlation=True,
    >>>                                            feature_histograms=True, cluster_analysis=True,
    >>>                                            stat_columns=['avg_trx', 'count_trx', 'max_trx', 'min_trx'])

    Args:
        :training_dataset: the training dataset to update the statistics for
        :training_dataset_version: the version of the training dataset (defaults to 1)
        :featurestore: the featurestore where the training dataset resides (defaults to the project's featurestore)
        :descriptive_statistics: a boolean flag whether to compute descriptive statistics (min,max,mean etc) for
                                 the featuregroup
        :feature_correlation: a boolean flag whether to compute a feature correlation matrix for the numeric columns
                              in the featuregroup
        :feature_histograms: a boolean flag whether to compute histograms for the numeric columns in the featuregroup
        :cluster_analysis: a boolean flag whether to compute cluster analysis for the numeric columns in
                           the featuregroup
        :stat_columns: a list of columns to compute statistics for (defaults to all columns that are numeric)
        :num_bins: number of bins to use for computing histograms
        :num_clusters: the number of clusters to use in clustering analysis (k-means)
        :corr_method: the method to compute feature correlation with (pearson or spearman)

    Returns:
        None
    """
    try:
        core._do_update_training_dataset_stats(training_dataset,
                                               core._get_featurestore_metadata(featurestore, update_cache=False),
                                               featurestore=featurestore,
                                               training_dataset_version=training_dataset_version,
                                               descriptive_statistics=descriptive_statistics,
                                               feature_correlation=feature_correlation,
                                               feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                               stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                                               num_clusters=num_clusters)
    except:
        # Retry with updated cache
        try:
            core._do_update_training_dataset_stats(training_dataset,
                                                   core._get_featurestore_metadata(featurestore, update_cache=True),
                                                   featurestore=featurestore,
                                                   training_dataset_version=training_dataset_version,
                                                   descriptive_statistics=descriptive_statistics,
                                                   feature_correlation=feature_correlation,
                                                   feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                                                   stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                                                   num_clusters=num_clusters)
        except Exception as e:
            raise StatisticsComputationError("There was an error in computing the statistics for training dataset: {}"
                                            " , with version: {} in featurestore: {}. "
                                            "Error: {}".format(training_dataset, training_dataset_version,
                                                               featurestore, str(e)))


def get_featuregroup_partitions(featuregroup, featurestore=None, featuregroup_version=1, dataframe_type="spark"):
    """
    Gets the partitions of a featuregroup

    Example usage:
    >>> partitions = featurestore.get_featuregroup_partitions("trx_summary_features")
    >>> #You can also explicitly define version, featurestore and type of the returned dataframe:
    >>> featurestore.get_featuregroup_partitions("trx_summary_features",
    >>>                                          featurestore=featurestore.project_featurestore(),
    >>>                                          featuregroup_version = 1,
    >>>                                          dataframe_type="spark")
     Args:
        :featuregroup: the featuregroup to get partitions for
        :featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        :featuregroup_version: the version of the featuregroup, defaults to 1
        :dataframe_type: the type of the returned dataframe (spark, pandas, python or numpy)

     Returns:
        a dataframe with the partitions of the featuregroup
     """
    if featurestore is None:
        featurestore = project_featurestore()
    try:
        # Try with cached metadata
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=False),
                                                    featurestore, featuregroup_version, dataframe_type)
    except:
        # Retry with updated cache
        return core._do_get_featuregroup_partitions(featuregroup,
                                                    core._get_featurestore_metadata(featurestore, update_cache=True),
                                                    featurestore, featuregroup_version, dataframe_type)


def visualize_featuregroup_distributions(featuregroup_name, featurestore=None, featuregroup_version=1, figsize=(16, 12),
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
        fs_utils._matplotlib_magic_reminder()

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
        fs_utils._matplotlib_magic_reminder()

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
        fs_utils._matplotlib_magic_reminder()

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
        fs_utils._matplotlib_magic_reminder()

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
        fs_utils._matplotlib_magic_reminder()

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
        fs_utils._matplotlib_magic_reminder()

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

def connect(host, project_name, port = 443, region_name = constants.AWS.DEFAULT_REGION):
    """
    Connects to a feature store from a remote environment such as Amazon SageMaker

    Example usage:
    >>> featurestore.connect("hops.site", "my_feature_store")

    Args:
        :host: the hostname of the Hopsworks cluster
        :project_name: the name of the project hosting the feature store to be used
        :port: the REST port of the Hopsworks cluster
        :region_name: The name of the AWS region in which the required secrets are stored

    Returns:
        None
    """
    # download certificates from AWS Secret manager to access Hive
    key_store = util.get_api_key_aws(project_name, 'key-store')
    util.write_b64_cert_to_bytes(key_store, path='keyStore.jks')
    trust_store = util.get_api_key_aws(project_name, 'trust-store')
    util.write_b64_cert_to_bytes(trust_store, path='trustStore.jks')
    cert_key = util.get_api_key_aws(project_name, 'cert-key')

    # write env variables
    os.environ["CERT_KEY"] = cert_key
    os.environ[constants.ENV_VARIABLES.REMOTE_ENV_VAR] = 'True'
    os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR] = host + ':' + str(port)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_NAME_ENV_VAR] = project_name
    os.environ[constants.ENV_VARIABLES.REGION_NAME_ENV_VAR] = region_name
    project_info = rest_rpc._get_project_info(project_name)
    os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] = str(project_info['projectId'])


def sync_hive_table_with_featurestore(featuregroup, description="", featurestore=None,
                                      featuregroup_version=1, jobs=[], feature_corr_data = None,
                                      featuregroup_desc_stats_data = None, features_histogram_data = None,
                                      cluster_analysis_data = None):
    """
    Synchronizes an existing Hive table with a Feature Store.

    Example usage:
    >>> # Save Hive Table
    >>> sample_df.write.mode("overwrite").saveAsTable("hive_fs_sync_example_1")
    >>> # Synchronize with Feature Store
    >>> featurestore.sync_hive_table_with_featurestore("hive_fs_sync_example", featuregroup_version=1)

    Args:
        :featuregroup: name of the featuregroup to synchronize with the hive table.
                       The hive table should have a naming scheme of featuregroup_version
        :description: description of the feature group
        :featurestore: the feature store where the hive table is stored
        :featuregroup_version: version of the feature group
        :jobs: jobs to compute this feature group (optional)
        :feature_corr_data: correlation statistics (optional)
        :featuregroup_desc_stats_data: descriptive statistics (optional)
        :features_histogram_data: histogram statistics (optional)
        :cluster_analysis_data: cluster analysis (optional)

    Returns:
        None
    """
    if featurestore is None:
        featurestore = project_featurestore()

    fs_utils._log("Synchronizing Hive Table: {} with Feature Store: {}".format(featuregroup, featurestore))

    try: # Try with cached metadata
        core._sync_hive_table_with_featurestore(
            featuregroup, core._get_featurestore_metadata(featurestore, update_cache=False),
            description=description, featurestore=featurestore, featuregroup_version=featuregroup_version,
            jobs=jobs, feature_corr_data=feature_corr_data, featuregroup_desc_stats_data=featuregroup_desc_stats_data,
            features_histogram_data=features_histogram_data, cluster_analysis_data=cluster_analysis_data)
    except: # Try again after updating the cache
        core._sync_hive_table_with_featurestore(
            featuregroup, core._get_featurestore_metadata(featurestore, update_cache=True),
            description=description, featurestore=featurestore, featuregroup_version=featuregroup_version,
            jobs=jobs, feature_corr_data=feature_corr_data, featuregroup_desc_stats_data=featuregroup_desc_stats_data,
            features_histogram_data=features_histogram_data, cluster_analysis_data=cluster_analysis_data)

    # update metadata cache
    try:
        core._get_featurestore_metadata(featurestore, update_cache=True)
    except:
        pass

    fs_utils._log("Hive Table: {} was successfully synchronized with Feature Store: {}".format(featuregroup,
                                                                                               featurestore))


def import_featuregroup(storage_connector, path, featuregroup, primary_key=None, description="", featurestore=None,
                        featuregroup_version=1, jobs=[],
                        descriptive_statistics=True, feature_correlation=True,
                        feature_histograms=True, cluster_analysis=True, stat_columns=None, num_bins=20,
                        corr_method='pearson', num_clusters=5, partition_by=[], data_format="parquet"):
    """
    Imports an external dataset of features into a feature group in Hopsworks.
    This function will read the dataset using spark and a configured storage connector (e.g to an S3 bucket)
    and then writes the data to Hopsworks Feature Store (Hive) and registers its metadata.

    Example usage:
    >>> featurestore.import_featuregroup(my_s3_connector_name, s3_path, featuregroup_name,
    >>>                                  data_format=s3_bucket_data_format)
    >>> # You can also be explicitly specify featuregroup metadata and what statistics to compute:
    >>> featurestore.import_featuregroup(my_s3_connector_name, s3_path, featuregroup_name, primary_key="id",
    >>>                                  description="trx_summary_features without the column count_trx",
    >>>                                  featurestore=featurestore.project_featurestore(),featuregroup_version=1,
    >>>                                  jobs=[], descriptive_statistics=False,
    >>>                                  feature_correlation=False, feature_histograms=False, cluster_analysis=False,
    >>>                                  stat_columns=None, partition_by=[], data_format="parquet")

    Args:
        :storage_connector: the storage connector used to connect to the external storage
        :path: the path to read from the external storage
        :featuregroup: name of the featuregroup to import the dataset into the featurestore
        :primary_key: primary key of the featuregroup
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

    Returns:
        None
    """
    # update metadata cache
    if featurestore is None:
        featurestore = project_featurestore()

    try: # try with metadata cache
        spark_df = core._do_get_external_featuregroup(storage_connector, path,
                                            core._get_featurestore_metadata(featurestore, update_cache=False),
                                            featurestore=featurestore, data_format=data_format)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by)
    except: # retry with updated metadata
        spark_df = core._do_get_external_featuregroup(storage_connector, path,
                                                      core._get_featurestore_metadata(featurestore, update_cache=False),
                                                      featurestore=featurestore, data_format=data_format)
        create_featuregroup(spark_df, featuregroup, primary_key=primary_key, description=description,
                            featurestore=featurestore, featuregroup_version=featuregroup_version, jobs=jobs,
                            descriptive_statistics=descriptive_statistics, feature_correlation=feature_correlation,
                            feature_histograms=feature_histograms, cluster_analysis=cluster_analysis,
                            stat_columns=stat_columns, num_bins=num_bins, corr_method=corr_method,
                            num_clusters=num_clusters, partition_by=partition_by)

    fs_utils._log("Feature group imported successfully")
