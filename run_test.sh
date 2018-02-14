#!/bin/bash

run_unit_test(){
    echo --------start---------
    ant runtest -Dtest=$1
    echo ---------end----------
    echo
}

run_system_test(){
    echo --------start---------
    ant runsystest -Dtest=$1 -Dsimpledb.Debug=1
    echo ---------end----------
    echo 
}
# basic operator
run_unit_test PredicateTest
run_unit_test JoinPredicateTest
run_unit_test FilterTest
run_unit_test JoinTest
run_system_test FilterTest
run_system_test JoinTest

# aggregator
run_unit_test StringAggregatorTest
run_unit_test IntegerAggregatorTest
run_unit_test AggregateTest
run_system_test AggregateTest

# insert and delete operator
run_unit_test HeapPageWriteTest
run_unit_test HeapFileWriteTest
run_unit_test InsertTest
run_unit_test TransactionTest
run_system_test InsertTest
run_system_test DeleteTest
run_system_test EvictionTest
run_system_test TransactionTest