#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: catalogue
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 06/08/19
# Date last modified: 06/08/19
# Project Name: matching-workflow

from pygres import Pygres
from psycopg2.errors import ForeignKeyViolation
import logging

logger = logging.getLogger()

class Catalogue(object):

    def __init__(self, config):
        self.config = config

    def connectdb(self, config):
        """Connects to the specific database."""
        return Pygres(dict(
            SQL_HOST=config['POSTGRESQL_HOST'],
            SQL_DB=config['POSTGRESQL_DB'],
            SQL_USER=config['POSTGRESQL_USER'],
            SQL_PASSWORD=config['POSTGRESQL_PASSWORD'],
            SQL_PORT=config['POSTGRESQL_PORT'],
        ))

    def match_products_by_item_uuid(self, found_matches_df):
        db = self.connectdb(self.config)
        total = len(found_matches_df)
        invalid_item_uuid = []
        bulk_query = []
        for i, row in found_matches_df.iterrows():
            try:
                logger.warning('Updating {}/{}'.format(i, total))
                query = """
                UPDATE product
                SET item_uuid = '{}'
                WHERE product_uuid::text = '{}' and source = '{}';
                """.format(row['item_uuid'], row['id_query'], row['retailer'])
                bulk_query.append(query)
                db.query(query)
            except ForeignKeyViolation:
                invalid_item_uuid.append(row['item_uuid'])
                logger.warning('invalid item_uuid {} at row {}/{}'.format(row['item_uuid'], i, total))
                db.query("ROLLBACK")
                db.commit()
            except Exception as e:
                logger.error(query)
                db.query("ROLLBACK")
                db.commit()
        return invalid_item_uuid
