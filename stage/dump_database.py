#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: read_unmatched_products
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/23/19
# Date last modified: 6/23/19
# Project Name: matching-framework

import utils
from extract import Product


class DumpDatabase(object):

    @classmethod
    def execute(cls):
        config_file = utils.read_yaml('config.yaml')
        retailer = config_file['dump_database']['retailer']
        product = Product(retailer)
        df = product.df
        df['product_name'] = df['name']
        df['text'] = df['name'].fillna('')
        return df
