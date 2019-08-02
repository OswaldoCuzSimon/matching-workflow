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


class GetRetailerProducts(object):

    @classmethod
    def execute(cls):
        config_file = utils.read_yaml('config.yaml')
        retailer = ['medimarket']
        product = Product(retailer)
        df = product.get_df_by_retailer('medimarket')
        df['product_name'] = df['name'].fillna('')
        df['gtin'] = df['gtin'].fillna('')
        df['id_query'] = df['product_uuid'].fillna('')
        return df

if __name__ == '__main__':
    #from stage.get_retailer_products import GetRetailerProducts

    stage = GetRetailerProducts()
    df = stage.execute()