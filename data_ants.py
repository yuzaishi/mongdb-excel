#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import argparse
import datetime
import fnmatch
import pluggy
from pymongo import MongoClient
import configparser
from migration.hookspec import ParserSpec, ExcelBasicPlugin, ExcelReadmePlugin

config = configparser.ConfigParser()
config.read('config.ini')
PLUGIN = {
    'ExcelBasicPlugin': ExcelBasicPlugin,
    'ExcelReadmePlugin': ExcelReadmePlugin
}


def load_source(path):
    assert path
    if not os.path.isabs(path):
        path = os.path.abspath(path)

    def _get_all_files(path):
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    yield os.path.join(root, file)
        else:
            yield path

    yield _get_all_files(path)


def import_to_db(path, hook, config):
    db = args.conf['DEFAULT']['data_set']
    ip_addr = args.conf['DEFAULT']['mongo_ip_addr']
    port = args.conf['DEFAULT']['mongo_port']

    if path.endswith(config.filter):
        client = MongoClient(ip_addr, int(port))
        mongo_db = client[db]
        mongo_collection = mongo_db[args.project]
        mongo_collection.insert_one({"filename": path})
    if path.endswith(config.index):
        hook.get_indexes(path=path, config=config)
        hook.parser(path=path, config=config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate test data source to database')
    parser.add_argument('-c', '--conf', nargs='?',
                        help="general configuration",
                        default=config
                        )
    parser.add_argument('-d', '--directory', nargs='+',
                        help='file system directory store original test data for a project',
                        default=[]
                        )
    parser.add_argument('-f', '--filter', nargs='+',
                        help='test resource type to be filter out and to be import to mongodb',
                        default=('mp3',)
                        )
    parser.add_argument('-i', '--index', nargs='+',
                        help='test resource index file type, such as excel',
                        default=('xlsx', 'xls')
                        )
    parser.add_argument('-s', '--sheet', nargs='+',
                        help='if index file is excel, specific sheet name',
                        default=[]
                        )
    parser.add_argument("--debug",
                        action="store_true",
                        help="enable debugging with maintainer email"
                        )
    args = parser.parse_args()

    pm = pluggy.PluginManager("index-parser")
    pm.add_hookspecs(ParserSpec)
    pm.register(PLUGIN.get(args.conf['DEFAULT']['plugin_name'])())

    for path in args.directory:
        items = load_source(path)
        for item in items:
            for value in item:
                import_to_db(value, pm.hook, args)


