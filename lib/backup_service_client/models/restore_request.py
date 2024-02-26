# coding: utf-8

"""
    Couchbase Backup Service API

    This is REST API allows users to remotely schedule and run backups, restores and merges as well as to explore various archives for all there Couchbase Clusters.  # noqa: E501

    OpenAPI spec version: 0.1.0
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""

import pprint
import re  # noqa: F401

import six


class RestoreRequest(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'target': 'str',
        'user': 'str',
        'password': 'str',
        'start': 'str',
        'end': 'str',
        'include_data': 'str',
        'exclude_data': 'str',
        'map_data': 'str',
        'auto_create_buckets': 'bool',
        'auto_remove_collections': 'bool',
        'enable_bucket_config': 'bool',
        'disable_views': 'bool',
        'disable_gsi_indexes': 'bool',
        'disable_ft': 'bool',
        'disable_data': 'bool',
        'disable_eventing': 'bool',
        'disable_analytics': 'bool',
        'force_updates': 'bool',
        'replace_ttl': 'str',
        'replace_ttl_with': 'str'
    }

    attribute_map = {
        'target': 'target',
        'user': 'user',
        'password': 'password',
        'start': 'start',
        'end': 'end',
        'include_data': 'include_data',
        'exclude_data': 'exclude_data',
        'map_data': 'map_data',
        'auto_create_buckets': 'auto_create_buckets',
        'auto_remove_collections': 'auto_remove_collections',
        'enable_bucket_config': 'enable_bucket_config',
        'disable_views': 'disable_views',
        'disable_gsi_indexes': 'disable_gsi_indexes',
        'disable_ft': 'disable_ft',
        'disable_data': 'disable_data',
        'disable_eventing': 'disable_eventing',
        'disable_analytics': 'disable_analytics',
        'force_updates': 'force_updates',
        'replace_ttl': 'replace_ttl',
        'replace_ttl_with': 'replace_ttl_with'
    }

    def __init__(self, target=None, user=None, password=None, start=None, end=None, include_data=None, exclude_data=None, map_data=None, auto_create_buckets=None, auto_remove_collections=None, enable_bucket_config=None, disable_views=None, disable_gsi_indexes=None, disable_ft=None, disable_data=None, disable_eventing=None, disable_analytics=None, force_updates=None, replace_ttl=None, replace_ttl_with=None):  # noqa: E501
        """RestoreRequest - a model defined in Swagger"""  # noqa: E501
        self._target = None
        self._user = None
        self._password = None
        self._start = None
        self._end = None
        self._include_data = None
        self._exclude_data = None
        self._map_data = None
        self._auto_create_buckets = None
        self._auto_remove_collections = None
        self._enable_bucket_config = None
        self._disable_views = None
        self._disable_gsi_indexes = None
        self._disable_ft = None
        self._disable_data = None
        self._disable_eventing = None
        self._disable_analytics = None
        self._force_updates = None
        self._replace_ttl = None
        self._replace_ttl_with = None
        self.discriminator = None
        if target is not None:
            self.target = target
        if user is not None:
            self.user = user
        if password is not None:
            self.password = password
        if start is not None:
            self.start = start
        if end is not None:
            self.end = end
        if include_data is not None:
            self.include_data = include_data
        if exclude_data is not None:
            self.exclude_data = exclude_data
        if map_data is not None:
            self.map_data = map_data
        if auto_create_buckets is not None:
            self.auto_create_buckets = auto_create_buckets
        if auto_remove_collections is not None:
            self.auto_remove_collections = auto_remove_collections
        if enable_bucket_config is not None:
            self.enable_bucket_config = enable_bucket_config
        if disable_views is not None:
            self.disable_views = disable_views
        if disable_gsi_indexes is not None:
            self.disable_gsi_indexes = disable_gsi_indexes
        if disable_ft is not None:
            self.disable_ft = disable_ft
        if disable_data is not None:
            self.disable_data = disable_data
        if disable_eventing is not None:
            self.disable_eventing = disable_eventing
        if disable_analytics is not None:
            self.disable_analytics = disable_analytics
        if force_updates is not None:
            self.force_updates = force_updates
        if replace_ttl is not None:
            self.replace_ttl = replace_ttl
        if replace_ttl_with is not None:
            self.replace_ttl_with = replace_ttl_with

    @property
    def target(self):
        """Gets the target of this RestoreRequest.  # noqa: E501

        The address of the cluster to restore to  # noqa: E501

        :return: The target of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._target

    @target.setter
    def target(self, target):
        """Sets the target of this RestoreRequest.

        The address of the cluster to restore to  # noqa: E501

        :param target: The target of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._target = target

    @property
    def user(self):
        """Gets the user of this RestoreRequest.  # noqa: E501

        The user to use for the restore  # noqa: E501

        :return: The user of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._user

    @user.setter
    def user(self, user):
        """Sets the user of this RestoreRequest.

        The user to use for the restore  # noqa: E501

        :param user: The user of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._user = user

    @property
    def password(self):
        """Gets the password of this RestoreRequest.  # noqa: E501

        The password to use for the restore  # noqa: E501

        :return: The password of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._password

    @password.setter
    def password(self, password):
        """Sets the password of this RestoreRequest.

        The password to use for the restore  # noqa: E501

        :param password: The password of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._password = password

    @property
    def start(self):
        """Gets the start of this RestoreRequest.  # noqa: E501

        The option to be passed to the restore --start  # noqa: E501

        :return: The start of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._start

    @start.setter
    def start(self, start):
        """Sets the start of this RestoreRequest.

        The option to be passed to the restore --start  # noqa: E501

        :param start: The start of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._start = start

    @property
    def end(self):
        """Gets the end of this RestoreRequest.  # noqa: E501

        The option to be passed to restore --end  # noqa: E501

        :return: The end of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._end

    @end.setter
    def end(self, end):
        """Sets the end of this RestoreRequest.

        The option to be passed to restore --end  # noqa: E501

        :param end: The end of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._end = end

    @property
    def include_data(self):
        """Gets the include_data of this RestoreRequest.  # noqa: E501

        The option to be passed to restore --include-data  # noqa: E501

        :return: The include_data of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._include_data

    @include_data.setter
    def include_data(self, include_data):
        """Sets the include_data of this RestoreRequest.

        The option to be passed to restore --include-data  # noqa: E501

        :param include_data: The include_data of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._include_data = include_data

    @property
    def exclude_data(self):
        """Gets the exclude_data of this RestoreRequest.  # noqa: E501

        The option to be passed to restore --exclude data  # noqa: E501

        :return: The exclude_data of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._exclude_data

    @exclude_data.setter
    def exclude_data(self, exclude_data):
        """Sets the exclude_data of this RestoreRequest.

        The option to be passed to restore --exclude data  # noqa: E501

        :param exclude_data: The exclude_data of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._exclude_data = exclude_data

    @property
    def map_data(self):
        """Gets the map_data of this RestoreRequest.  # noqa: E501

        The option to be passed to restore --map-data  # noqa: E501

        :return: The map_data of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._map_data

    @map_data.setter
    def map_data(self, map_data):
        """Sets the map_data of this RestoreRequest.

        The option to be passed to restore --map-data  # noqa: E501

        :param map_data: The map_data of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._map_data = map_data

    @property
    def auto_create_buckets(self):
        """Gets the auto_create_buckets of this RestoreRequest.  # noqa: E501

        Enables the restore option --auto-create-buckets  # noqa: E501

        :return: The auto_create_buckets of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._auto_create_buckets

    @auto_create_buckets.setter
    def auto_create_buckets(self, auto_create_buckets):
        """Sets the auto_create_buckets of this RestoreRequest.

        Enables the restore option --auto-create-buckets  # noqa: E501

        :param auto_create_buckets: The auto_create_buckets of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._auto_create_buckets = auto_create_buckets

    @property
    def auto_remove_collections(self):
        """Gets the auto_remove_collections of this RestoreRequest.  # noqa: E501

        Enables the restore option --auto-remove-collections  # noqa: E501

        :return: The auto_remove_collections of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._auto_remove_collections

    @auto_remove_collections.setter
    def auto_remove_collections(self, auto_remove_collections):
        """Sets the auto_remove_collections of this RestoreRequest.

        Enables the restore option --auto-remove-collections  # noqa: E501

        :param auto_remove_collections: The auto_remove_collections of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._auto_remove_collections = auto_remove_collections

    @property
    def enable_bucket_config(self):
        """Gets the enable_bucket_config of this RestoreRequest.  # noqa: E501

        Enables the restore option --enable-bucket-config  # noqa: E501

        :return: The enable_bucket_config of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._enable_bucket_config

    @enable_bucket_config.setter
    def enable_bucket_config(self, enable_bucket_config):
        """Sets the enable_bucket_config of this RestoreRequest.

        Enables the restore option --enable-bucket-config  # noqa: E501

        :param enable_bucket_config: The enable_bucket_config of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._enable_bucket_config = enable_bucket_config

    @property
    def disable_views(self):
        """Gets the disable_views of this RestoreRequest.  # noqa: E501

        TEnables the restore option --disable-views  # noqa: E501

        :return: The disable_views of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_views

    @disable_views.setter
    def disable_views(self, disable_views):
        """Sets the disable_views of this RestoreRequest.

        TEnables the restore option --disable-views  # noqa: E501

        :param disable_views: The disable_views of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_views = disable_views

    @property
    def disable_gsi_indexes(self):
        """Gets the disable_gsi_indexes of this RestoreRequest.  # noqa: E501

        Enables the restore option --disable-gsi-indexes  # noqa: E501

        :return: The disable_gsi_indexes of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_gsi_indexes

    @disable_gsi_indexes.setter
    def disable_gsi_indexes(self, disable_gsi_indexes):
        """Sets the disable_gsi_indexes of this RestoreRequest.

        Enables the restore option --disable-gsi-indexes  # noqa: E501

        :param disable_gsi_indexes: The disable_gsi_indexes of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_gsi_indexes = disable_gsi_indexes

    @property
    def disable_ft(self):
        """Gets the disable_ft of this RestoreRequest.  # noqa: E501

        Enables the restore option --disable-full-text-search  # noqa: E501

        :return: The disable_ft of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_ft

    @disable_ft.setter
    def disable_ft(self, disable_ft):
        """Sets the disable_ft of this RestoreRequest.

        Enables the restore option --disable-full-text-search  # noqa: E501

        :param disable_ft: The disable_ft of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_ft = disable_ft

    @property
    def disable_data(self):
        """Gets the disable_data of this RestoreRequest.  # noqa: E501

        Enables the restore option --disable-data  # noqa: E501

        :return: The disable_data of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_data

    @disable_data.setter
    def disable_data(self, disable_data):
        """Sets the disable_data of this RestoreRequest.

        Enables the restore option --disable-data  # noqa: E501

        :param disable_data: The disable_data of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_data = disable_data

    @property
    def disable_eventing(self):
        """Gets the disable_eventing of this RestoreRequest.  # noqa: E501

        Enables the restore option --disable-eventing  # noqa: E501

        :return: The disable_eventing of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_eventing

    @disable_eventing.setter
    def disable_eventing(self, disable_eventing):
        """Sets the disable_eventing of this RestoreRequest.

        Enables the restore option --disable-eventing  # noqa: E501

        :param disable_eventing: The disable_eventing of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_eventing = disable_eventing

    @property
    def disable_analytics(self):
        """Gets the disable_analytics of this RestoreRequest.  # noqa: E501

        Enables the restore option --disable-analytics  # noqa: E501

        :return: The disable_analytics of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._disable_analytics

    @disable_analytics.setter
    def disable_analytics(self, disable_analytics):
        """Sets the disable_analytics of this RestoreRequest.

        Enables the restore option --disable-analytics  # noqa: E501

        :param disable_analytics: The disable_analytics of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._disable_analytics = disable_analytics

    @property
    def force_updates(self):
        """Gets the force_updates of this RestoreRequest.  # noqa: E501

        Enables the restore option --force-updates  # noqa: E501

        :return: The force_updates of this RestoreRequest.  # noqa: E501
        :rtype: bool
        """
        return self._force_updates

    @force_updates.setter
    def force_updates(self, force_updates):
        """Sets the force_updates of this RestoreRequest.

        Enables the restore option --force-updates  # noqa: E501

        :param force_updates: The force_updates of this RestoreRequest.  # noqa: E501
        :type: bool
        """

        self._force_updates = force_updates

    @property
    def replace_ttl(self):
        """Gets the replace_ttl of this RestoreRequest.  # noqa: E501

        The option passed to --replace-ttl  # noqa: E501

        :return: The replace_ttl of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._replace_ttl

    @replace_ttl.setter
    def replace_ttl(self, replace_ttl):
        """Sets the replace_ttl of this RestoreRequest.

        The option passed to --replace-ttl  # noqa: E501

        :param replace_ttl: The replace_ttl of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._replace_ttl = replace_ttl

    @property
    def replace_ttl_with(self):
        """Gets the replace_ttl_with of this RestoreRequest.  # noqa: E501

        The option passed to --replace-ttl-with  # noqa: E501

        :return: The replace_ttl_with of this RestoreRequest.  # noqa: E501
        :rtype: str
        """
        return self._replace_ttl_with

    @replace_ttl_with.setter
    def replace_ttl_with(self, replace_ttl_with):
        """Sets the replace_ttl_with of this RestoreRequest.

        The option passed to --replace-ttl-with  # noqa: E501

        :param replace_ttl_with: The replace_ttl_with of this RestoreRequest.  # noqa: E501
        :type: str
        """

        self._replace_ttl_with = replace_ttl_with

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list([x.to_dict() if hasattr(x, "to_dict") else x for x in value])
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict([(item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item for item in list(value.items())])
            else:
                result[attr] = value
        if issubclass(RestoreRequest, dict):
            for key, value in list(self.items()):
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, RestoreRequest):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
