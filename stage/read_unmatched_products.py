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
from extract.read_file import XLSX


class ReadUnmatchedProducts(object):

    @classmethod
    def execute(cls):
        config_file = utils.read_yaml('config.yaml')
        unmatching_file_kargs = config_file['unmatching_file']['kargs']

        path = config_file['unmatching_file']['path']
        id_col = config_file['unmatching_file']['id_col']
        gtin_col = config_file['unmatching_file']['gtin_col']
        product_namme_col = config_file['unmatching_file']['product_namme_col']

        xlsx = XLSX(path, id_col, gtin_col, product_namme_col)
        df = xlsx.df(**unmatching_file_kargs)
        return df
