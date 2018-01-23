simple-db
=========

Code for all 6.830 labs will be available in this repo. Once you have set up your class repo, you pull lab code from here.

Directions for Repo Setup
-------------------------

Directions can be [here](https://github.com/MIT-DB-Class/course-info-2017)

Lab Submission
-----

Instructions for labs (including how to submit answers) are [here](https://github.com/MIT-DB-Class/course-info-2017)

==========================================
==========================================
simple DB structure:

Type:
  1. INT_TYPE
  2. STRING_TYPE

TupleDesc:
  1. TDItem
    1. Type (fieldType)
    2. String (fieldName)

RecordId (to a specific page of a specific table):

Tuple:
  1. TupleDesc (schema)
  2. RecordId
  3. array of Fields (Field)

