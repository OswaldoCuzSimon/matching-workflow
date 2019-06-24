#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: config
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 30/04/19
# Date last modified: 30/04/19
# Project Name: geolocated-price-comparison
#%%
from dotenv import load_dotenv
import os

load_dotenv()
config = {
    "POSTGRESQL_HOST": os.getenv("POSTGRESQL_HOST"),
    "POSTGRESQL_PORT": os.getenv("POSTGRESQL_PORT"),
    "POSTGRESQL_USER": os.getenv("POSTGRESQL_USER"),
    "POSTGRESQL_PASSWORD": os.getenv("POSTGRESQL_PASSWORD"),
    "POSTGRESQL_DB": os.getenv("POSTGRESQL_DB"),
}
