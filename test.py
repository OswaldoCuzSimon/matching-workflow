#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: test
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/23/19
# Date last modified: 6/23/19
# Project Name: matching-framework

#%%
import utils
from extract.read_file import XLSX
import pandas as pd

gtin_df = pd.read_csv('gtin_match_products_df.csv', converters={'gtin': str})
text_df = pd.read_csv('text_match_products_df.csv', converters={'gtin': str})
product_df = pd.read_csv('product.csv', converters={'gtin': str})

#%%
text_df['product_uuid'] = text_df['id_result']
short_text_df = pd.merge(text_df, product_df, on='product_uuid', how='inner')
short_text_df = short_text_df[['gtin', 'item_uuid', 'name']]
short_gtin_df = gtin_df[['gtin', 'item_uuid', 'name']]

match_df = pd.concat([short_gtin_df, short_text_df])