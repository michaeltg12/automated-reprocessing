""" ********** module  name **********

Author: Michael Giansiracusa
Email: giansiracumt@ornl.gov

Purpose:

Note:

Classes:

Methods:

Example:

Attributes:

Todo:

"""

import io_methods
import argparse
import logging
import netCDF4
import pandas
import time
import os

module_path = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("visual")

import plotly
from plotly.graph_objs import *

plotly.tools.set_credentials_file(username='michaeltg12',
                                  api_key='OzX6qqKOOwYZzQT2kvn2')
plotly.tools.set_config_file(world_readable=True,
                             sharing='public')

input_dir = module_path + '/input/'
output_dir = module_path + '/output/'

io_manager = io_methods.IOMethods(input_dir, output_dir, logging.DEBUG)

file = '/Users/ofg/production/testing/automated-reprocessing/output/nsaskyrad60sC1.b1.19990403.000000.cdf2.pkl'
data = io_manager.load_obj(file)
