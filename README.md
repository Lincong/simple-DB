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

8585347487

Type:
  1. INT_TYPE
  2. STRING_TYPE

TupleDesc:
  1. TDItem
    1. Type (fieldType)
    2. String (fieldName)

RecordId (to a specific page of a specific table):

Field:
    1. StringField
    2. IntField

Tuple:
  1. TupleDesc (schema)
  2. RecordId
    1. pageID
    2. tupleNum
  3. array of Fields (Field)

Catalog:
  1. String (table name)
  2. String (primary key)
  3. DbFile
    1. ID


HeapFile (one for each table):
  1. set of HeapPage (each page has BufferPool.DEFAULT_PAGE_SIZE bytes)

HeapPage:
  1. a set of slots (一个slot一个tuple, tuple有固定的大小 )
  2. a header (bitmap)
    class fields:
        1. pageID
        2. tuple description
        3. header (bitmap)
        4. array of Tuples
        5. number of slots (maximum number of tuples)
        6. number of empty slots

Predicate:
    1. 1 field (field ID that is the index of field in the Tuple object. Tuple里面的第几个field)
    2. 1 operator (op)
    3. 1 operand (Field)


"->" means "use" 

HeapFile iterator -> Bufferpool.getPage() -> HeapFile readPage (read from disk)

HeapFile insert   -> Bufferpool.getPage() -> HeapFile readPage

HeapFile delete   -> Bufferpool.getPage() -> HeapFile readPage

Bufferpool insertTuple -> HeapFile insertTuple -> 
    1. HeapPage insert (on a free one or a new page)
    2. writePage(). (write to disk)

Bufferpool deleteTuple -> HeapFile deleteTuple -> 
    1. HeapPage insert ()
    2. put page in the buffer pool



concurrency control
---
1. HeapFile.insertTuple(), HeapFile.deleteTuple() and HeapFile.iterator()
should both use BufferPool.getPage()

2. BufferPool.insertTuple() and BufferPool.deleteTupe() call markDirty()

3. Release all locks associated with a transaction after it has committed or aborted to ensure strict 2PL

4. 