# coding: utf-8

"""
    Couchbase Backup Service API

    This is REST API allows users to remotely schedule and run backups, restores and merges as well as to explore various archives for all there Couchbase Clusters.  # noqa: E501

    OpenAPI spec version: 0.1.0
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""



import re  # noqa: F401

# python 2 and python 3 compatibility library
import six

from backup_service_client.api_client import ApiClient


class ActiveRepositoryApi(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    Ref: https://github.com/swagger-api/swagger-codegen
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    def cluster_self_repository_active_id_archive_post(self, id, **kwargs):  # noqa: E501
        """Archives the repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_archive_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param ArchiveRequest body:
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_archive_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_archive_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_archive_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Archives the repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_archive_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param ArchiveRequest body:
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id', 'body']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_archive_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_archive_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/archive', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_backup_post(self, id, **kwargs):  # noqa: E501
        """Triggers a one off backup for the specified repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_backup_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param BackupRequest body:
        :return: OneOffTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_backup_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_backup_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_backup_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Triggers a one off backup for the specified repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_backup_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param BackupRequest body:
        :return: OneOffTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id', 'body']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_backup_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_backup_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/backup', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='OneOffTaskResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_backups_backup_name_delete(self, id, backup_name, **kwargs):  # noqa: E501
        """Deletes the backup  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_backups_backup_name_delete(id, backup_name, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param str backup_name: The backup to be deleted (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_backups_backup_name_delete_with_http_info(id, backup_name, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_backups_backup_name_delete_with_http_info(id, backup_name, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_backups_backup_name_delete_with_http_info(self, id, backup_name, **kwargs):  # noqa: E501
        """Deletes the backup  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_backups_backup_name_delete_with_http_info(id, backup_name, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param str backup_name: The backup to be deleted (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id', 'backup_name']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_backups_backup_name_delete" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_backups_backup_name_delete`")  # noqa: E501
        # verify the required parameter 'backup_name' is set
        if ('backup_name' not in params or
                params['backup_name'] is None):
            raise ValueError("Missing the required parameter `backup_name` when calling `cluster_self_repository_active_id_backups_backup_name_delete`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501
        if 'backup_name' in params:
            path_params['backupName'] = params['backup_name']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/backups/{backupName}', 'DELETE',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_merge_post(self, id, **kwargs):  # noqa: E501
        """Triggers a one off merge for the specified repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_merge_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param MergeRequest body:
        :return: OneOffTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_merge_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_merge_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_merge_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Triggers a one off merge for the specified repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_merge_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param MergeRequest body:
        :return: OneOffTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id', 'body']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_merge_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_merge_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/merge', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='OneOffTaskResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_pause_post(self, id, **kwargs):  # noqa: E501
        """Pauses the active repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_pause_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_pause_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_pause_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_pause_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Pauses the active repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_pause_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_pause_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_pause_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/pause', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_post(self, id, **kwargs):  # noqa: E501
        """Creates an active repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param CreateActiveRepositoryRequest body:
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Creates an active repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :param CreateActiveRepositoryRequest body:
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id', 'body']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def cluster_self_repository_active_id_resume_post(self, id, **kwargs):  # noqa: E501
        """Resumes the paused repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_resume_post(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.cluster_self_repository_active_id_resume_post_with_http_info(id, **kwargs)  # noqa: E501
        else:
            (data) = self.cluster_self_repository_active_id_resume_post_with_http_info(id, **kwargs)  # noqa: E501
            return data

    def cluster_self_repository_active_id_resume_post_with_http_info(self, id, **kwargs):  # noqa: E501
        """Resumes the paused repository  # noqa: E501

        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.cluster_self_repository_active_id_resume_post_with_http_info(id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str id: The repository ID. (required)
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['id']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method cluster_self_repository_active_id_resume_post" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'id' is set
        if ('id' not in params or
                params['id'] is None):
            raise ValueError("Missing the required parameter `id` when calling `cluster_self_repository_active_id_resume_post`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/cluster/self/repository/active/{id}/resume', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)
