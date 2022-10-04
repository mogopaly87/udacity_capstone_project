from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators

class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [operators.stage_to_redshift, operators.upload_stations_to_s3]