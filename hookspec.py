import os
import re
import xlrd
import csv
import pluggy
import tempfile


spec = pluggy.HookspecMarker("index-parser")
impl = pluggy.HookimplMarker("index-parser")


def replace_suffix(path, index_type, expect):
    rep = dict(zip(index_type, [expect]*len(index_type)))
    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    return pattern.sub(lambda m: rep[re.escape(m.group(0))], path)


class ParserSpec:

    @spec
    def get_indexes(self, path, config):
        """get indexes
        """

    @spec
    def parser(self, path, config):
        """parser a path with index type
        """


class ExcelBasicPlugin:

    @impl
    def parser(self, path, config):
        index_type, sheet, title = config.index, config.sheet, config.title

        def _convert_to_csv(path, sheet, title):
            wb = xlrd.open_workbook(path)
            try:
                sh = wb.sheet_by_name(sheet)
                print(sh.row_values(title))
                fileds = sh.row_values(title)
                out_path = replace_suffix(path, index_type, 'csv')
                with open(out_path, 'w', encoding='utf-8') as dest_csv:
                    wr = csv.writer(dest_csv, quoting=csv.QUOTE_ALL)
                    for row_num in range(title, sh.nrows):
                        wr.writerow(sh.row_values(row_num))
                return out_path, fileds
            except xlrd.biffh.XLRDError:
                print("No sheet {} in file {}".format(sheet, path))

        def _upload_csv(path, fileds, config):
            import subprocess
            basename = os.path.basename(path).split('.')[0]
            ipaddr = config.conf['DEFAULT']['mongo_ip_addr']
            port = config.conf['DEFAULT']['mongo_port']
            db = config.conf['DEFAULT']['data_set']
            collection = basename + '_' + config.sheet

            # delete db before update new csv file
            subprocess.run(["mongo",
                            "--host",
                            ipaddr,
                            "--port",
                            port,
                            db,
                            "--eval",
                            "db.dropDatabase()"
                            ])

            subprocess.run(["mongoimport",
                            "--host",
                            ipaddr,
                            "--port",
                            port,
                            "--db",
                            db,
                            "--collection",
                            collection,
                            "--type",
                            "csv",
                            # "--mode",
                            # "upsert",
                            # "--upsertFields",
                            # ','.join(fileds),
                            "--headerline",
                            "--file",
                            path
                            ])
        try:
            path, fields = _convert_to_csv(path, sheet, title)
            _upload_csv(path, fields, config)
        except TypeError:
            pass


class ExcelReadmePlugin:

    def __init__(self):
        self.indexes = []

    @spec
    def get_indexes(self, path, config):
        """get indexes
        """
        index_type, sheet = config.index, config.sheet

        wb = xlrd.open_workbook(path)
        sh = wb.sheet_by_name(sheet)
        out_path = replace_suffix(path, index_type, 'csv')
        with open(out_path, 'w', encoding='utf-8') as dest_csv:
            wr = csv.writer(dest_csv, quoting=csv.QUOTE_ALL)
            for row_num in range(2, 13):
                wr.writerow(sh.row_values(row_num))
                self.indexes.append(sh.row_values(row_num))

    @impl
    def parser(self, path, config):
        index_type, sheet, title = config.index, config.sheet, config.title

        def _convert_to_csv(path, sheet, title):
            wb = xlrd.open_workbook(path)
            sh = wb.sheet_by_name(sheet)
            print(sh.row_values(title))
            fileds = sh.row_values(title)
            out_path = replace_suffix(path, index_type, 'csv')
            with open(out_path, 'w', encoding='utf-8') as dest_csv:
                wr = csv.writer(dest_csv, quoting=csv.QUOTE_ALL)
                for row_num in range(title, sh.nrows):
                    wr.writerow(sh.row_values(row_num))
            return out_path, fileds

        def _upload_csv(path, fileds, config):
            import subprocess
            basename = os.path.basename(path).split('.')[0]
            ipaddr = config.conf['DEFAULT']['mongo_ip_addr']
            port = config.conf['DEFAULT']['mongo_port']
            db = config.conf['DEFAULT']['data_set']
            collection = config.conf['MAPPER'].get(basename) or basename

            # delete db before update new csv file
            subprocess.run(["mongo",
                            "--host",
                            ipaddr,
                            "--port",
                            port,
                            db,
                            "--eval",
                            "db.dropDatabase()"
                            ])

            subprocess.run(["mongoimport",
                            "--host",
                            ipaddr,
                            "--port",
                            port,
                            "--db",
                            db,
                            "--collection",
                            collection,
                            "--type",
                            "csv",
                            # "--mode",
                            # "upsert",
                            # "--upsertFields",
                            # ','.join(fileds),
                            "--headerline",
                            "--file",
                            path
                            ])

        for indexed_path in self.indexes:
            # to be doing
            basename, full_path = indexed_path[1], indexed_path[8]
            path, fields = _convert_to_csv(full_path, sheet, title)
            _upload_csv(path, fields, config)











