add_executable(tpcc_backend backend.cpp tpcc.cpp)
target_link_libraries(tpcc_backend PUBLIC dtx_tpcc)

add_executable(tpcc_bench bench.cpp tpcc.cpp)
target_link_libraries(tpcc_bench PUBLIC dtx_tpcc)
