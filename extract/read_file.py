#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: read_file
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/22/19
# Date last modified: 6/22/19
# Project Name: matching-framework

import pandas as pd


class XLSX(object):
    
    def __init__(self, path, id_col, gtin_col, product_namme_col):
        self.name = path
        self.id_col = id_col
        self.gtin_col = gtin_col
        self.product_name_col = product_namme_col

    def df(self, **kargs):
        kargs.update({'converters': {self.gtin_col: str}})
        df_ = pd.read_excel(self.name, **kargs)
        df_["gtin"] = df_[self.gtin_col]
        if self.id_col == 'index':
            df_["id_query"] = df_.index
        else:
            df_["id_query"] = df_[self.id_col]
        df_.gtin = df_.gtin.str.zfill(14)
        df_['product_name'] = df_[self.product_name_col]
        df_ = df_.loc[:, ~df_.columns.str.contains('^Unnamed')]
        df_ = df_.fillna('')
        return df_
