#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: matching_gtin
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/26/19
# Date last modified: 6/26/19
# Project Name: matching-framework

import pandas as pd
import utils


class MatchingGtin(object):

    @classmethod
    def match_by_gtin(cls, original_df1, original_df2):
        df1 = original_df1.copy()
        df2 = original_df2.copy()
        df1 = df1[df1.gtin.str.isdigit()]
        df2 = df2[df2.gtin.str.isdigit()]
        df1.gtin = df1.gtin.astype(int)
        df2.gtin = df2.gtin.astype(int)
        df1 = df1[~df1.gtin.isnull()]
        df2 = df2[~df2.gtin.isnull()]
        df1 = df1[(df1.gtin != '')]
        df2 = df2[(df2.gtin != '')]
        result = pd.merge(df1, df2, on='gtin', how='inner')
        result = result.drop_duplicates('id_query')
        return result

    @classmethod
    def execute(cls, unmatched_products_df, product_df):
        config_file = utils.read_yaml('config.yaml')
        gtin_match_products_df = cls.match_by_gtin(unmatched_products_df, product_df[['item_uuid', 'gtin', 'name', 'product_uuid']])

        return gtin_match_products_df
