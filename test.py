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
from load.catalogue import Catalogue

gtin_df = pd.read_csv('gtin_match_products_df.csv', converters={'gtin': str})
text_df = pd.read_csv('text_match_products_df.csv', converters={'gtin': str})
product_df = pd.read_csv('product.csv', converters={'gtin': str})



#%%
text_df['product_uuid'] = text_df['id_result']
short_text_df = pd.merge(text_df, product_df, on='product_uuid', how='inner')
short_text_df = short_text_df[['gtin', 'item_uuid', 'name', 'id_query', 'id_result']]
gtin_df['name'] = gtin_df['name_x']
gtin_df['id_result'] = gtin_df['product_uuid_y']
short_gtin_df = gtin_df[['gtin', 'item_uuid', 'name', 'id_query', 'id_result']]

match_df = pd.concat([short_gtin_df, short_text_df])
match_df.gtin = match_df.gtin.str.zfill(14)
#%%
new_item_df = match_df[match_df.item_uuid.isnull()]
update_item_df = match_df[~match_df.item_uuid.isnull()]
update_item_df['retailer'] = 'medimarket'
#%%


#%%
from config import config
catalogue = Catalogue(config)
#wrong_items = catalogue.match_products_by_item_uuid(update_item_df)

#%%
query = """
INSERT INTO item(item_uuid, gtin, name, description, last_modified)
VALUES('{}', '{}', '{}', '{}', '{}') 
"""
import numpy as np
unmatched_product_df = pd.read_csv('unmatched_products_clean_file.csv', converters={'gtin': str})
new_item_df.gtin = new_item_df.gtin.replace('0'*14, np.nan)
new_item_df['product_uuid'] = new_item_df['id_query']
unmatched_product_df = unmatched_product_df[['product_uuid', 'description']]
new_item_df['product_uuid'] = new_item_df['id_query']
short_text_df = pd.merge(new_item_df, unmatched_product_df, on='product_uuid', how='inner')
