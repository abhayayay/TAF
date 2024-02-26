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


class ServiceConfiguration(object):
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
        'history_rotation_period': 'int',
        'history_rotation_size': 'int'
    }

    attribute_map = {
        'history_rotation_period': 'history_rotation_period',
        'history_rotation_size': 'history_rotation_size'
    }

    def __init__(self, history_rotation_period=None, history_rotation_size=None):  # noqa: E501
        """ServiceConfiguration - a model defined in Swagger"""  # noqa: E501
        self._history_rotation_period = None
        self._history_rotation_size = None
        self.discriminator = None
        if history_rotation_period is not None:
            self.history_rotation_period = history_rotation_period
        if history_rotation_size is not None:
            self.history_rotation_size = history_rotation_size

    @property
    def history_rotation_period(self):
        """Gets the history_rotation_period of this ServiceConfiguration.  # noqa: E501

        How often the task history is rotated in days  # noqa: E501

        :return: The history_rotation_period of this ServiceConfiguration.  # noqa: E501
        :rtype: int
        """
        return self._history_rotation_period

    @history_rotation_period.setter
    def history_rotation_period(self, history_rotation_period):
        """Sets the history_rotation_period of this ServiceConfiguration.

        How often the task history is rotated in days  # noqa: E501

        :param history_rotation_period: The history_rotation_period of this ServiceConfiguration.  # noqa: E501
        :type: int
        """

        self._history_rotation_period = history_rotation_period

    @property
    def history_rotation_size(self):
        """Gets the history_rotation_size of this ServiceConfiguration.  # noqa: E501

        After how many MB to rotate the task history.  # noqa: E501

        :return: The history_rotation_size of this ServiceConfiguration.  # noqa: E501
        :rtype: int
        """
        return self._history_rotation_size

    @history_rotation_size.setter
    def history_rotation_size(self, history_rotation_size):
        """Sets the history_rotation_size of this ServiceConfiguration.

        After how many MB to rotate the task history.  # noqa: E501

        :param history_rotation_size: The history_rotation_size of this ServiceConfiguration.  # noqa: E501
        :type: int
        """

        self._history_rotation_size = history_rotation_size

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
        if issubclass(ServiceConfiguration, dict):
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
        if not isinstance(other, ServiceConfiguration):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
