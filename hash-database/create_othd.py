#! /usr/bin/python3
################################################################################
# create_othd
#
# Copyright 2025 Opentext Corp. All Rights Reserved
################################################################################

import uuid
import sqlite3
import csv

import os
import hashlib
import time

################################################################################
# BaseInputDatabase
#   The base class all input database types are derived from.
#
# To subclass BaseInputDatabase, you need to override available_columns and
#   file_infos appropriate to your implementation.
################################################################################
class BaseInputDatabase:
    POSSIBLE_COLUMNS = set(['size', 'sha1', 'md5'])
    OPTIMAL_COLUMN_ORDER = ['size', 'sha1', 'md5']

    ################################################################################
    # validate_column_list - quickly check we don't have an invalid column setup
    ################################################################################
    @staticmethod
    def validate_column_list(columns):
        if len(columns) == 0:
            raise ValueError("At least one column must be specified")
        #You can just remove this exception if you really want to use a size only db for
        # some reason
        if len(columns) == 1 and 'size' in columns:
            raise ValueError("Size-only databases are not advised.")
        for a in columns:
            if a not in BaseInputDatabase.POSSIBLE_COLUMNS:
                raise ValueError(f"Column {a} not allowed")

    ################################################################################
    # __init__ - default init the main header values.
    ################################################################################
    def __init__(self):
        self._name = ""
        self._description = ""
        self._uuid = uuid.uuid4()

    ################################################################################
    # name - property
    ################################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    ################################################################################
    # description - property
    ################################################################################
    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    ################################################################################
    # uuid - property
    ################################################################################
    @property
    def uuid(self):
        return self._uuid

    ################################################################################
    # available_columns - VIRTUAL
    #   This property shall be a set of column names your database contains.
    ################################################################################
    @property
    def available_columns(self):
        raise NotImplementedError

    ################################################################################
    # ordered_column_subset
    #   forces 'available_column's into the preferred, optimal order.  This is
    #   important mostly so that 'size' is first if available.
    ################################################################################
    def ordered_column_subset(self, subset):
        if subset == None:
            subset = self.available_columns
        BaseInputDatabase.validate_column_list(subset)
        return [a for a in self.OPTIMAL_COLUMN_ORDER if a in subset]

    ################################################################################
    # Get the columns, ordered, with their types for table creation.
    ################################################################################
    def columns_with_type(self, columns_override=None):
        columns = self.ordered_column_subset(columns_override)
        BaseInputDatabase.validate_column_list(columns)
        tr = []
        for i in columns:
            if i == 'size':
                tr += ['size INT NOT NULL']
            elif i == 'md5':
                tr += ['md5 BLOB NOT NULL']
            elif i == 'sha1':
                tr += ['sha1 BLOB NOT NULL']
            else:
                raise ValueError(f"Could not process column {i}")
        return tr

    ################################################################################
    # file_infos - VIRTUAL
    #   This property shall be a generator that generates objects.  The generated
    #   objects need to support the function:
    #
    #   get_column(self, column_name)
    #
    #   For each column name returned by 'available_columns'
    ################################################################################
    @property
    def file_infos(self):
        raise NotImplementedError

################################################################################
# FolderAsInputDatabase
#   A simple subclass of BaseInputDatabase that recursively walks a given
#   directory and provides rows for the files in it.
################################################################################
class FolderAsInputDatabase(BaseInputDatabase):

    ################################################################################
    # __init__
    #   Allows caller to specify what columns
    ################################################################################
    def __init__(self, folder, columns=['size', 'md5', 'sha1']):
        BaseInputDatabase.validate_column_list(columns)
        super().__init__()
        self._folder = folder
        self._columns = set(columns)

    ################################################################################
    # available_columns
    #   The database 'contains' all of these because it's just going to calculate
    #   them as we go.
    ################################################################################
    @property
    def available_columns(self):
        return self._columns

    ################################################################################
    # FileInfo
    #   This class's file_infos generated objects.
    ################################################################################
    class FileInfo:
        def __init__(self, filename):
            self.filename = filename
        def get_column(self, column_name):
            if column_name == 'size':
                return os.path.getsize(self.filename)
            if column_name == 'md5':
                return hashlib.md5(open(self.filename, 'rb').read()).digest()
            if column_name == 'sha1':
                return hashlib.sha1(open(self.filename, 'rb').read()).digest()
            raise ValueError(f"Column `{column_name}` not supported")

    ################################################################################
    # file_infos
    ################################################################################
    @property
    def file_infos(self):
        for item in os.walk(self._folder):
            for filename in item[2]:
                yield self.FileInfo(os.path.join(item[0], filename))

################################################################################
# HashListAsInput
#   A simple subclass to allow importing files where each row is one text hash
#   value of a given type.  Also suppports files where each row is one text
#   number that is a file size, though that seems fairly useless
################################################################################
class HashListAsInputDatabase(BaseInputDatabase):

    ################################################################################
    # __init__
    ################################################################################
    def __init__(self, filename, column):
        super().__init__()
        self._filename = filename
        self._column = column

    ################################################################################
    # available_columns
    #   Just listify our one column.
    ################################################################################
    @property
    def available_columns(self):
        return set([self._column])

    ################################################################################
    # FileInfo
    #   This class's file_infos generated objects.
    ################################################################################
    class FileInfo:
        def __init__(self, column, line):
            self._line = line.strip()
            self._column = column

        def get_column(self, column_name):
            assert column_name == self._column, "Asked for invalid column"
            if column_name == 'size':
                return int(self._line)
            elif column_name == 'md5':
                if len(self._line) != 32:
                    raise ValueError(f"String '{self._line}' is not 32 characters long")
            elif column_name == 'sha1':
                if len(self._line) != 40:
                    raise ValueError(f"String '{self._line}' is not 40 characters long ")

            return bytes.fromhex(self._line)

    ################################################################################
    # file_infos
    ################################################################################
    @property
    def file_infos(self):
        with open(self._filename, 'r') as f:
            while True:
                line = f.readline()
                if len(line) == 0:
                    return
                yield self.FileInfo(self._column, line)

################################################################################
# NsrlRdsInputDatabase
#   Import a National Software Reference Library database of files.
################################################################################
class NsrlRdsInputDatabase(BaseInputDatabase):

    ################################################################################
    # __init__
    ################################################################################
    def __init__(self, db_path):
        super().__init__()
        if not os.path.exists(db_path):
            raise ValueError(f"Database {db_path} does not exist")
        self._db_path = db_path
        self.connection = sqlite3.connect(db_path)

        self.set_description_from_db()

    ################################################################################
    # __del__
    # Close out our connection to the NSRL db when done.
    ################################################################################
    def __del__(self):
        self.connection.close()

    ################################################################################
    # set_description_from_db
    #   Fill in a default description from some helpful rows from the NSRL db.
    ################################################################################
    def set_description_from_db(self):
        cur = self.connection.cursor()
        cur.execute("SELECT release_date, description FROM VERSION LIMIT 1;")
        fetched = cur.fetchone()
        if fetched is not None:
            self.description = f"Built from {fetched[1]} released on {fetched[0]}"
        cur.close()

    ################################################################################
    # available_columns
    ################################################################################
    @property
    def available_columns(self):
        return set(['size', 'md5', 'sha1'])

    ################################################################################
    # FileInfo
    #   This class's file_infos generated objects.
    ################################################################################
    class FileInfo:
        def __init__(self, line):
            self._line = line
        def get_column(self, column_name):
            if column_name == 'size':
                return self._line[0]
            if column_name == 'sha1':
                return bytes.fromhex(self._line[1])
            if column_name == 'md5':
                return bytes.fromhex(self._line[2])
            raise ValueError(f"Column `{column_name}` not supported")

    ################################################################################
    # file_infos
    ################################################################################
    @property
    def file_infos(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT file_size, sha1, md5 FROM FILE;")
        while True:
            lines = cursor.fetchmany()
            if len(lines) == 0:
                return
            for line in lines:
                yield self.FileInfo(line)

################################################################################
# NsrlCaidInputDatabase
#   Import a National Software Reference Library CAID database
################################################################################
class NsrlCaidInputDatabase(BaseInputDatabase):
    ################################################################################
    # __init__
    ################################################################################
    def __init__(self, db_path, desired_categories=None):
        super().__init__()
        if not os.path.exists(db_path):
            raise ValueError(f"Database {db_path} does not exist")
        self._db_path = db_path
        self._desired_categories = desired_categories

    ################################################################################
    # available_columns
    ################################################################################
    @property
    def available_columns(self):
        return set(['size', 'md5', 'sha1'])

    ################################################################################
    # is_category_desired
    ################################################################################
    def is_category_desired(self, category_number):
        if self._desired_categories is None:
            return True
        return category_number in self._desired_categories

    ################################################################################
    # FileInfo
    #   This class's file_infos generated objects.
    ################################################################################
    class FileInfo:
        def __init__(self, line):
            parts = [a.split(':') for a in line.split(',')]
            self._dict = {}
            for x in parts:
                self._dict[x[0]] = x[1]

        @property
        def valid(self):
            return True

        @property
        def category(self):
            return int(self._dict['"Category"'])

        def get_column(self, column_name):
            if column_name == 'size':
                return int( self._dict['"MediaSize"'].strip('"') )
            if column_name == 'sha1':
                return bytes.fromhex( self._dict['"SHA1"'].strip('"') )
            if column_name == 'md5':
                return bytes.fromhex( self._dict['"MD5"'].strip('"') )
            raise ValueError(f"Column `{column_name}` not supported")

    ################################################################################
    # file_infos
    ################################################################################
    @property
    def file_infos(self):
        DATA_LINE_HEADER = (' ' * 6) + '],'
        with open(self._db_path, 'r') as file:
            while True:
                line = file.readline()
                if line == "":
                    return
                if not line.startswith(DATA_LINE_HEADER):
                    continue
                meat = line[len(DATA_LINE_HEADER):-1]
                info = NsrlCaidInputDatabase.FileInfo(meat)
                if info.valid and self.is_category_desired(info.category):
                    yield info

################################################################################
# CsvInputDatabase
#   This csv implementation assumes the column names are exactly the ones from
#   a OTFE-ggenerated logical imaging csv.  If this is not the case, you will
#   need to edit at least
#       CsvInputDatabase->FileInfo->__init__
#   to get the fields correctly for your csv.
################################################################################
class CsvInputDatabase(BaseInputDatabase):
    def __init__(self, csv_path, dialect='excel'):
        super().__init__()

        self._csv_path = csv_path
        self._dialect = dialect

        self._cols = set()

        with open(self._csv_path, 'r', newline='') as f:
            csv_reader = csv.DictReader(f, dialect=self._dialect)
            for row_dict in csv_reader:
                info = CsvInputDatabase.FileInfo(row_dict)
                if info.valid:
                    self._cols = info.columns
                    break
        if len(self._cols) == 0:
            raise ValueError(
                "No CSV columns found.  Check that this script supports your column names")

    ################################################################################
    # available_columns
    ################################################################################
    @property
    def available_columns(self):
        return self._cols

    ################################################################################
    # FileInfo
    #   This class's file_infos generated objects.
    ################################################################################
    class FileInfo:
        def __init__(self, line_dict):
            self._valid = line_dict.get('Type', '') != 'Directory'
            self.size = line_dict.get('Filesize', '') or None
            self.sha1 = line_dict.get('SHA1 Hash', '') or None
            self.md5 = line_dict.get('MD5 Hash', '') or None

        @property
        def valid(self):
            return self._valid

        @property
        def columns(self):
            tr = []
            if self.size: tr += ['size']
            if self.sha1: tr += ['sha1']
            if self.md5:  tr += ['md5']
            return set(tr)

        def get_column(self, column_name):
            if column_name == 'size':
                return int(self.size)
            if column_name == 'sha1':
                return bytes.fromhex(self.sha1)
            if column_name == 'md5':
                return bytes.fromhex(self.md5)
            raise ValueError(f"Column `{column_name}` not supported")

    ################################################################################
    # file_infos
    ################################################################################
    @property
    def file_infos(self):
        with open(self._csv_path, 'r', newline='') as f:
            csv_reader = csv.DictReader(f, dialect=self._dialect)
            for row_dict in csv_reader:
                ty = CsvInputDatabase.FileInfo(row_dict)
                if ty.valid:
                    yield ty

################################################################################
# create_and_fill_header_table
################################################################################
def create_and_fill_header_table(input_db, output_db_cursor):
    if type(input_db.uuid) is not uuid.UUID:
        raise ValueError("UUID type not appropriate, dbs must have good UUIDs")

    name = input_db.name
    desc = input_db.description
    my_uuid = input_db.uuid.bytes

    output_db_cursor.execute(
        "CREATE TABLE header (name TEXT, description TEXT, uuid BLOB);")
    output_db_cursor.execute("INSERT INTO header (name, description, uuid) VALUES (?, ?, ?);",
        (name, desc, my_uuid))

################################################################################
# create_files_table
################################################################################
def create_files_table(input_db, output_db_cursor, columns_override=None):
    columns = input_db.columns_with_type(columns_override)
    if len(columns) == 0:
        raise ValueError("Input DB did not support any columns")
    columns_text = ', '.join(columns)
    output_db_cursor.execute(f"CREATE TABLE files ({columns_text});")

################################################################################
# fill_files_table
################################################################################
def fill_files_table(input_db, output_db_cursor, columns_override=None, print_status=False):
    column_names = input_db.ordered_column_subset(columns_override)
    column_names_str = ', '.join(column_names)
    column_q_marks = ', '.join(['?' for a in column_names])
    insert_statement = f"INSERT INTO files ({column_names_str}) VALUES ({column_q_marks});"


    if print_status:
        TIME_PRINT_GRANULARITY = 0.5
        next_print_time = time.time() + TIME_PRINT_GRANULARITY
        files_processed = 0
        print(f"Entries processed: {files_processed:,}", end='\r')

    for file in input_db.file_infos:
        values = [file.get_column(a) for a in column_names]
        output_db_cursor.execute(insert_statement, values)
        if print_status:
            files_processed += 1
            current_time = time.time()
            if current_time > next_print_time:
                print(f"Entries processed: {files_processed:,}", end='\r')
                next_print_time = current_time + TIME_PRINT_GRANULARITY
    if print_status:
        print(f"Entries processed: {files_processed:,}")

################################################################################
# create_indexes
################################################################################
def create_indexes(input_db, output_db_cursor, columns_override=None):
    column_names = input_db.ordered_column_subset(columns_override)
    if len(column_names) > 0:
        cols = ', '.join(column_names)
        output_db_cursor.execute(f"CREATE INDEX all_index ON files ({cols});")

    output_db_cursor.execute("PRAGMA optimize;")

################################################################################
# write_to_output_db
################################################################################
def write_to_output_db(input_db, output_path, print_status=False):
    con = sqlite3.connect(output_path)
    cur = con.cursor()

    cur.execute("PRAGMA application_id = 0x4f544844;")
    cur.execute("PRAGMA user_version = 1;")

    create_and_fill_header_table(input_db, cur)
    create_files_table(input_db, cur)
    fill_files_table(input_db, cur, print_status=print_status)

    create_indexes(input_db, cur)

    con.commit()

################################################################################
# __main__
################################################################################
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Create Opentext Forensic Equipment compatible file hash database")
    parser.add_argument('input_path', help='Input file or folder')

    parser.add_argument('-t', '--type',
                        choices=['folder', 'md5_list', 'sha1_list', 'nsrl_rds', 'nsrl_caid', 'csv'],
                        default='folder',
                        help='Type of input')

    parser.add_argument('output_file',
            help='SQLite database ready for OpenText Forensic Equipment logical imaging')
    parser.add_argument('-n', '--name' , help='Name to give database (max 63 characters)', default='')
    parser.add_argument('-d', '--description' ,
                        help='description to give database (max 1023 characters)',
                        default='')
    parser.add_argument('--csv-dialect' ,
                        choices=csv.list_dialects(),
                        default='excel',
                        help='CSV dialect for csv input types')
    args = parser.parse_args()

    if os.path.exists(args.output_file):
        print(f"Output file `{args.output_file}` already exists")
        exit(-1)
    if len(args.name) > 63:
        print('Names longer than 63 characters may not be fully viewable by clients')
        exit(-1)
    if len(args.description) > 1023:
        print('Descriptions longer than 1023 characters may not be fully viewable by clients')
        exit(-1)

    if args.type == 'folder':
        input_db = FolderAsInputDatabase(args.input_path)
    elif args.type == 'md5_list':
        input_db = HashListAsInputDatabase(args.input_path, 'md5')
    elif args.type == 'sha1_list':
        input_db = HashListAsInputDatabase(args.input_path, 'sha1')
    elif args.type == 'nsrl_rds':
        input_db = NsrlRdsInputDatabase(args.input_path)
    elif args.type == 'nsrl_caid':
        input_db = NsrlCaidInputDatabase(args.input_path)
    elif args.type == 'csv':
        input_db = CsvInputDatabase(args.input_path, args.csv_dialect)
    else:
        raise NotImplementedError

    if len(args.name) > 0:
        input_db.name = args.name
    if len(args.description) > 0:
        input_db.description = args.description
    write_to_output_db(input_db, args.output_file, print_status = True)
    exit(0)

