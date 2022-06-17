#!/bin/bash 

touch blocks.sqlite transactions.sqlite logs.sqlite

cat transform_testdata.sql | sqlite3 testdata.sqlite
