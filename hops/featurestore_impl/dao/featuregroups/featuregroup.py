from hops import constants
from hops.featurestore_impl.dao.common.featurestore_entity import FeaturestoreEntity
from hops.featurestore_impl.dao.featuregroups.cached_featuregroup import CachedFeaturegroup
from hops.featurestore_impl.dao.featuregroups.on_demand_featuregroup import OnDemandFeaturegroup


class Featuregroup(FeaturestoreEntity):
    """
    Represents an individual featuregroup in the featurestore
    """

    def __init__(self, featuregroup_json):
        """
        Initialize the feature group from JSON payload

        Args:
            :featuregroup_json: JSON representation of the featuregroup, returned from Hopsworks REST API
        """
        if constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION in featuregroup_json:
            self.description = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_DESCRIPTION]
        else:
            self.description = ""
        self.features = self._parse_features(featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_FEATURES])
        self.created = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_CREATED]
        self.creator = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_CREATOR]
        self.name = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_NAME]
        self.version = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_VERSION]
        self.id = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_ID]
        self.featuregroup_type = featuregroup_json[constants.REST_CONFIG.JSON_FEATUREGROUP_TYPE]
        if(self.featuregroup_type == constants.REST_CONFIG.JSON_FEATUREGROUP_ON_DEMAND_TYPE):
            self.on_demand_featuregroup = OnDemandFeaturegroup(featuregroup_json)
        if(self.featuregroup_type == constants.REST_CONFIG.JSON_FEATUREGROUP_CACHED_TYPE):
            self.cached_featuregroup = CachedFeaturegroup(featuregroup_json)


    def __lt__(self, other):
        return self.name.__lt__(other.name)

    def is_online(self):
        """
        Returns: true if the feature group has online serving enabled, otherwise false
        """
        return (self.featuregroup_type == constants.REST_CONFIG.JSON_FEATUREGROUP_CACHED_TYPE and
                self.cached_featuregroup.online_enabled)
