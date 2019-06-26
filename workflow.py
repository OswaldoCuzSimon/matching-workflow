#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: workflow
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 6/23/19
# Date last modified: 6/23/19
# Project Name: matching-framework

import luigi
import csv
import stage


class CleanUnmatchedProductsFile(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('unmatched_products_clean_file.csv')

    def run(self):
        stage_ = stage.ReadUnmatchedProducts()
        df = stage_.execute()
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)


class DumpDatabase(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('product.csv')

    def run(self):
        stage_ = stage.DumpDatabase()
        df = stage_.execute()
        df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL)


if __name__ == '__main__':
    luigi.run()
