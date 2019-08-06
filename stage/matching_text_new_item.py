#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: matching_text
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/29/19
# Date last modified: 6/29/19
# Project Name: matching-framework

import json
import logging
import pandas as pd
from text_matcher.text_matcher import TextMatcher
from text_matcher.bag_of_words import normalize
import utils


logger = logging.getLogger()


class MatchingTextNewItem(object):

    @classmethod
    def match_by_text(cls, unmatched_products_df, product_df, precision):
        txm = TextMatcher(lang='spanish', stems=True)
        txm.init_normalizer()

        logger.info("Starting match by text df1 size: {} df2 size: {}".format(
            len(unmatched_products_df), len(product_df)))
        product_df['text'] = product_df['name'].fillna('')
        product_df['text'] = product_df['text'].apply(lambda z: normalize(txm.norm, z))

        unmatched_products_df['text'] = unmatched_products_df.product_name
        unmatched_products_df['id'] = unmatched_products_df.id_query
        #unmatched_products_df = unmatched_products_df[(
        #                                                  unmatched_products_df.gtin.isnull()) | (unmatched_products_df.gtin == '')]
        product_df['id'] = product_df['product_uuid']

        logger.info("Preparing list of queries")
        query_list = json.loads(unmatched_products_df[['id', 'text']].to_json(orient='records'))
        txm.init_text_matcher(product_df[['text', 'id']], match_key='id', dims=300)
        logger.info("Query list size: {}".format(len(query_list)))

        score_columns = ["text_cosine", "text_dice", "text_jaccard", "text_overlap", "text_tdlr"]
        frames = []
        total = len(query_list)
        for i, query in enumerate(query_list):
            matches = txm.query_matches(query)
            logger.warning('query {}/{}'.format(i, total))
            matches['mean'] = matches[score_columns].mean(axis=1)
            df_of_matching = matches[matches['mean'] >= precision]
            frames.append(df_of_matching)
        res_df = pd.concat(frames)

        text_match_products_df = res_df[res_df['mean'] >= precision]
        text_match_products_df.to_csv('text_match_products_df.csv', index=False)
        text_match_products_df = text_match_products_df.drop_duplicates('id_query')

        return text_match_products_df

    @classmethod
    def execute(cls, unmatched_products_df, product_df):
        config_file = utils.read_yaml('config.yaml')
        precision = config_file['matching_text']['precision']
        text_match_products_df = cls.match_by_text(unmatched_products_df, product_df, precision)
        return text_match_products_df
