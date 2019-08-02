#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: workflow
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/23/19
# Date last modified: 6/23/19
# Project Name: matching-framework

import pandas as pd
import luigi
import csv
import logging
import stage


logger = logging.getLogger()


class CleanUnmatchedProductsFile(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('unmatched_products_clean_file.csv')

    def run(self):
        logger.info("Starting clean_unmatched_products_file Stage")
        #stage_ = stage.ReadUnmatchedProducts()
        stage_ = stage.GetRetailerProducts()
        df = stage_.execute()
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)
        logger.info("Finishing clean_unmatched_products_file Stage")


class DumpDatabase(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('product.csv')

    def run(self):
        logger.info("Starting dump_database Stage")
        stage_ = stage.DumpDatabase()
        df = stage_.execute()
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)
        logger.info("Finishing dump_database Stage")


class MatchingGtin(luigi.Task):

    def requires(self):
        return [CleanUnmatchedProductsFile(), DumpDatabase()]

    def output(self):
        return luigi.LocalTarget('gtin_match_products_df.csv')

    def run(self):
        input = self.input()
        logger.info("Starting matching_gtin stage")
        unmatched_products_df = pd.read_csv(input[0].open('r'), converters={'gtin': str})
        product_df = pd.read_csv(input[1].open('r'), converters={'gtin': str})
        stage_ = stage.MatchingGtin()
        df = stage_.execute(unmatched_products_df, product_df)

        logger.info(df.head())
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)
        logger.info("Finishing matching_gtin stage")


class MatchingText(luigi.Task):

    def requires(self):
        return [CleanUnmatchedProductsFile(), DumpDatabase()]

    def output(self):
        return luigi.LocalTarget('text_match_products_df.csv')

    def run(self):
        input = self.input()
        logger.info("Starting matching_text stage")
        unmatched_products_df = pd.read_csv(input[0].open('r'), converters={'gtin': str})
        product_df = pd.read_csv(input[1].open('r'), converters={'gtin': str})
        stage_ = stage.MatchingText()
        df = stage_.execute(unmatched_products_df, product_df)

        logger.info(df.head())
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)
        logger.info("Finishing matching_text stage")

if __name__ == '__main__':
    luigi.run()
