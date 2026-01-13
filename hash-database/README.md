# OTFE Hash Databases

OTFE-supported hash databases must be SQLite databases with two tables, the Header Table and the Files Table.

TX2 will recursively search the '`/otfe_hash_databases/`' folder of all non-Source filesystems for databases it can use.

## Header Table

The header table, 'header', is formed as:

`CREATE TABLE header (name TEXT NOT NULL, description TEXT NOT NULL, uuid BLOB NOT NULL);`

These columns must contain:

- name:  Some string name.  The logical imaging engine will cut off this field after 63 characters.
- description: Some string description.  The logical imaging engine will cut off this field after 1023 characters.
- uuid:  The raw 16 bytes of a UUID.  This field must be unique, if TX2 finds duplicate UUIDs between two databases it will refuse to use either database.

The table should have only one row.  If more than one row exists, the final one will be assumed to be accurate, as selected by:

`SELECT name, description, uuid FROM header ORDER BY rowid DESC LIMIT 1;`

*Note that rowids are not guaranteed to increase as you add more rows.  It's best to make sure header only has one row.*

## Files Table

The files table, 'files', is laid out as:

`CREATE TABLE files (size INT NOT NULL, sha1 BLOB NOT NULL, md5 BLOB NOT NULL);`

Not all three columns need to exist, but at least one must.

- size: The size of the file, in bytes
- sha1: The sha1 hash, as a binary blob (NOT hexified)
- md5: The md5 hash, as a binary blob (NOT hexified)

It is recommended for performance that your database contains the size and one hash column, preferably the strongest hash available.  Multiple hash columns will slow you down calculating hashes, and will not help your search unless one hash happens to have conflicts, which should be very rare, especially if you have size as well.

## Recommended Index

If 'size' is available, the logical imaging engine will verify that at least one file that size exists in the database before it bothers calculating either hash.  So it is **very strongly** recommended that your database has an index on size available to it.  It's also recommended to have an index on all columns available, so something like

`CREATE INDEX all_index ON files (size, sha1, md5);`

Will achieve both ends.  This works because indexes are stored by sorting on the first column first, then the second, then the third.

## Application Id

The 'Application Id' metadata field should be set to 0x4f544844, which is "OTHD" in ASCII and 1,330,923,588 in base 10.

*Note that the creation scripts currently do set this, but the logical imaging engine does not currently check it.*

`PRAGMA application_id = 0x4f544844;`

## User Version

The 'User Version' metadata field should be 1.  This number will be incremented if OpenText ever changes database schemas.

*Note that the creation scripts currently do set this, but the logical imaging engine does not currently check it.*

`PRAGMA user_version = 1;`

## How does the logical imaging engine use these databases to find matching files?

The logical imaging engine opens the database in 'immutable' mode, which means that the database **must** not be modified while the logical imaging engine is running.

If the size column exists, the logical imaging engine first runs

`SELECT EXISTS (SELECT 1 FROM files where size = ?);`

With the given size.  If this results in '0', then we know no file exists in the database and return that fact immediately.

If the first select results in not '0', or if no size column exists, we then calculate whatever file hashes we need and run

`SELECT EXISTS (SELECT 1 FROM files where size = ? AND sha1 = ? AND md5 = ?);`

With all columns.