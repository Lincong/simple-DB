#!/bin/bash

run_unit_test(){
    echo --------start---------
    ant runtest -Dtest=$1
    echo ---------end----------
    echo
}

run_system_test(){
    echo --------start---------
    ant runsystest -Dtest=$1
    echo ---------end----------
    echo 
}

#run_unit_test HeapPageWriteTest
#run_unit_test HeapFileWriteTest
#run_unit_test InsertTest
#run_system_test InsertTest
#run_system_test DeleteTest
#run_system_test EvictionTest
run_system_test AggregateTest
