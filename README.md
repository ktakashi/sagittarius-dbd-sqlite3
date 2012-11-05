# Sagittarius DBD SQLite3

This provides SQLite3 binding and DBD library for Sagittarius.

## Install

The build process trys to find SQLite3 runtime library from your platform,
however I haven't tested much platform. So if you have a trouble comment out
the line 9 to 14 in `CMakeLists.txt` and write your own SQLite3 runtime path.

## The difference between builtin ODBC library

Sagittarius has ODBC library if the platform supports and it also provides DBD.
Even though both have the same DBI interface but some of the behaviours or
returning data are not the same.

Following describes the major differences.

 * BLOBs are bytevector
 * Commit and rollback are not supported (always auto commit)

NOTE: This library works Sagittarius 0.3.8 (current HEAD) or later.