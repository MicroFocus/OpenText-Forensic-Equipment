#! /usr/bin/python3
################################################################################
# describe_othd
#
# Copyright 2025 Opentext Corp. All Rights Reserved
################################################################################

import uuid
import sqlite3

import os

################################################################################
# get_header_info
################################################################################
def get_header_info(db_cursor):
    db_cursor.execute("SELECT name, description, uuid FROM header ORDER BY rowid DESC LIMIT 1;")
    result = db_cursor.fetchall()[0]
    return {
        'name': result[0],
        'description': result[1],
        'uuid': result[2],
    }

################################################################################
# get_files_columns
################################################################################
def get_files_columns(db_cursor):
    db_cursor.execute("PRAGMA table_info(files);")
    rows = db_cursor.fetchall()
    return [ a[1] for a in rows ]

################################################################################
# get_files_index_names
################################################################################
def get_files_index_names(db_cursor):
    db_cursor.execute("PRAGMA index_list(files);")
    rows = db_cursor.fetchall()
    return [ a[1] for a in rows ]

################################################################################
# get_files_index_info
################################################################################
def get_files_index_info(db_cursor, index_name):
    db_cursor.execute(f"PRAGMA index_info({index_name});")
    rows = db_cursor.fetchall()
    return [ a[2] for a in rows ]

################################################################################
# get_files_index_infos
################################################################################
def get_files_index_infos(db_cursor):
    tr = {}
    for a in get_files_index_names(db_cursor):
        tr[a] = get_files_index_info(db_cursor, a)
    return tr

################################################################################
# get_files_count
################################################################################
def get_files_count(db_cursor):
    db_cursor.execute("SELECT COUNT(1) from files;")
    return db_cursor.fetchall()[0][0]

################################################################################
# get_db_application_id
################################################################################
def get_db_application_id(db_cursor):
    db_cursor.execute("PRAGMA application_id;")
    return db_cursor.fetchall()[0][0]


################################################################################
# get_db_version
################################################################################
def get_db_version(db_cursor):
    db_cursor.execute("PRAGMA user_version;")
    return db_cursor.fetchall()[0][0]

################################################################################
# HashDb
################################################################################
class HashDb:
    def __init__(self, db_path):
        self._db_path = db_path

        con = sqlite3.connect(db_path)
        cur = con.cursor()

        header_info = get_header_info(cur)
        self.name = header_info['name']
        self.description = header_info['description']
        self.uuid = uuid.UUID(bytes=header_info['uuid'])

        self.files_columns = get_files_columns(cur)
        self.files_index_infos = get_files_index_infos(cur)
        self.files_count = get_files_count(cur)

        self.application_id = get_db_application_id(cur)
        self.db_version = get_db_version(cur)
        cur.close()
        con.close()

    ################################################################################
    # application_id_is_correct
    ################################################################################
    @property
    def application_id_is_correct(self):
        return self.application_id == 0x4f544844

    ################################################################################
    # pretty_application_id
    ################################################################################
    @property
    def pretty_application_id(self):
        if self.application_id_is_correct:
            return hex(self.application_id)
        return f"!!!{hex(self.application_id)}!!!"

    ################################################################################
    # db_version_understood_by_this_script
    ################################################################################
    @property
    def db_version_understood_by_this_script(self):
        return self.db_version == 1

    ################################################################################
    # has_ideal_index
    ################################################################################
    @property
    def has_ideal_index(self):
        def index_is_ideal(index_id):
            index = self.files_index_infos[index_id]
            if len(index) != len(self.files_columns):
                return False
            if 'size' in self.files_columns:
                return index[0] == 'size'
            return True

        return any(map(index_is_ideal, self.files_index_infos))

    ################################################################################
    # pretty_db_version
    ################################################################################
    @property
    def pretty_db_version(self):
        if self.db_version_understood_by_this_script:
            return str(self.db_version)
        return f'!!!{self.db_version}!!!'

    ################################################################################
    # pretty_name
    ################################################################################
    @property
    def pretty_name(self):
        if self.name:
            return f'"{self.name}"'
        return "<None>"

    ################################################################################
    # pretty_description
    ################################################################################
    @property
    def pretty_description(self):
        if self.description:
            return f'"{self.description}"'
        return "<None>"

    ################################################################################
    # pretty_has_ideal_index
    ################################################################################
    @property
    def pretty_has_ideal_index(self):
        if self.has_ideal_index:
            return "Yes"
        return "No"

    ################################################################################
    # print_detailed_description
    ################################################################################
    def print_detailed_description(self):
        print(f"Application Id: {self.pretty_application_id}")
        print(f"Database Version: {self.pretty_db_version}")
        print(f"Name: {self.pretty_name}")
        print( "Description:")
        print(f"    {self.pretty_description}")
        print(f"UUID: {self.uuid}")
        print(f"Columns: {', '.join(self.files_columns)}")
        print(f"Has Ideal Index: {self.pretty_has_ideal_index}")
        print( "Indexes:")
        for a in self.files_index_infos:
            print(f"    {a}: {', '.join(self.files_index_infos[a])}")
        if self.files_count is not None:
            print(f"Entries: {self.files_count:,}")

    ################################################################################
    # get_and_print_sample_rows
    ################################################################################
    def get_and_print_sample_rows(self):
        con = sqlite3.connect(self._db_path)
        cur = con.cursor()
        print('\t'.join(self.files_columns))
        cur.execute(f"SELECT {','.join(self.files_columns)} FROM files ORDER BY rowid DESC LIMIT 20;")

        for i in cur.fetchall():
            cols = []
            for c in i:
                if type(c) == bytes:
                    cols += [str(c.hex())]
                else:
                    cols += [str(c)]
            print(', '.join(cols))



    ################################################################################
    # print_json
    ################################################################################
    def print_json(self):
        print("{")
        print(f'    "application_id": {self.application_id},')
        print(f'    "db_version": {self.db_version},')
        if self.name:
            print(f'    "name": "{self.name}",')
        if self.description:
            print(f'    "description": "{self.description}",')
        print(f'    "uuid": "{self.uuid}",')
        quoted_cols = map(lambda a : '"' + a + '"', self.files_columns)
        print(f'    "columns": [ {", ".join(quoted_cols)} ]')
        print("}")


################################################################################
# __main__
################################################################################
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Describe Opentext Forensic Equipment compatible file hash database")
    parser.add_argument("db_path", help='valid, compatible database file path')
    parser.add_argument("-j", '--json',
                        help='Print json rather than human readable',
                        action='store_true')

    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"Database file `{args.db_path}` does not exist")
        exit(-1)

    db = HashDb(args.db_path)

    if args.json:
        db.print_json()
    else:
        db.print_detailed_description()
        db.get_and_print_sample_rows()
    exit(0)
