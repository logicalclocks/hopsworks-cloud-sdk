"""
Exceptions thrown by the feature store python client
"""

class FeaturegroupNotFound(Exception):
    """This exception will be raised if a requested featuregroup cannot be found"""


class FeatureNotFound(Exception):
    """This exception will be raised if a requested feature cannot be found"""


class FeatureNameCollisionError(Exception):
    """This exception will be raised if a requested feature cannot be uniquely identified in the feature store"""


class InferJoinKeyError(Exception):
    """This exception will be raised if a join key for a featurestore query cannot be inferred"""


class TrainingDatasetNotFound(Exception):
    """This exception will be raised if a requested training dataset cannot be found"""


class FeatureVisualizationError(Exception):
    """This exception will be raised if there is an error in visualization feature statistics"""


class FeatureDistributionsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature distributions
    for a feature group or training dataset for which the distributions have not been computed
    """

class FeatureCorrelationsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature correlations
    for a feature group or training dataset for which the correlations have not been computed
    """

class FeatureClustersNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature clusters
    for a feature group or training dataset for which the clusters have not been computed
    """

class DescriptiveStatisticsNotComputed(Exception):
    """
    This exception will be raised if the user tries to visualize feature clusters
    for a feature group or training dataset for which the clusters have not been computed
    """


class StorageConnectorNotFound(Exception):
    """This exception will be raised if a requested storage connector cannot be found"""


class CannotGetPartitionsOfOnDemandFeatureGroup(Exception):
    """
    This exception will be raised if the user calls featurestore.get_featuregroup_partitions(fg1)
    where fg1 is an on-demand feature group
    """


class OnlineFeaturestorePasswordOrUserNotFound(Exception):
    """
    This exception will be raised if the user tries to do an operation on the online feature store but a
    user/password for the online featurestore was not found.
    """
