#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: price
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 02/05/19
# Date last modified: 02/05/19
# Project Name: geolocated-price-comparison

from config import config
import psycopg2
import pandas.io.sql as sqlio


class Product(object):

    def __init__(self, retailer_keys):
        self.retailer_keys = retailer_keys
        self.db = None

    def connect(self):
        if not self.db:
            self.db = psycopg2.connect(
                "host='{}' port={} dbname='{}' user={} password={}".format(
                    config['POSTGRESQL_HOST'],
                    config['POSTGRESQL_PORT'], config['POSTGRESQL_DB'],
                    config['POSTGRESQL_USER'], config['POSTGRESQL_PASSWORD']))

    @property
    def df(self):
        return self.get_df_by_stores(self.retailer_keys)

    @df.setter
    def df(self, new_df):
        pass

    def get_df_by_stores(self, retailer_keys):
        self.connect()
        # replace method when retailer list has length 1
        string_list = tuple(self.retailer_keys).__str__().replace(',)', ')')
        query = """
        select product_uuid, item_uuid, name, gtin, source 
        from product 
        where item_uuid is not NULL and source in {};""".format(string_list)
        df = sqlio.read_sql_query(query, self.db)

        self.db = None
        return df

    def get_df_by_retailer(self, retailer_key):
        self.connect()
        query = """
        select product_uuid, product_id, name, gtin, source 
        from product 
        where source = '{}';""".format(retailer_key)
        df = sqlio.read_sql_query(query, self.db)

        self.db = None
        return df
