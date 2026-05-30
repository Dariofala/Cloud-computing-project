# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.inline_response200 import InlineResponse200  # noqa: E501
from swagger_server.models.inline_response2001 import InlineResponse2001  # noqa: E501
from swagger_server.models.inline_response202 import InlineResponse202  # noqa: E501
from swagger_server.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_job_post(self):
        """Test case for job_post

        Launch a job
        """
        query_string = [('framework', 'framework_example'),
                        ('input_file', 'input_file_example')]
        response = self.client.open(
            '/job',
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_jobs_get(self):
        """Test case for jobs_get

        Get the status of all recent jobs
        """
        response = self.client.open(
            '/jobs',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_jobs_job_id_get(self):
        """Test case for jobs_job_id_get

        Get the status and results of a specific job
        """
        response = self.client.open(
            '/jobs/{jobId}'.format(job_id='job_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
