#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: __init__.py
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/23/19
# Date last modified: 6/23/19
# Project Name: matching-framework

import yaml


def read_yaml(path):
    try:
        with open(path, 'r') as stream:
            yaml_dict = yaml.safe_load(stream)
            return yaml_dict
    except yaml.YAMLError as exc:
        print(exc)
