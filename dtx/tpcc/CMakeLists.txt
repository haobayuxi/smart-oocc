add_executable(tpcc_backend backend.cpp)
target_link_libraries(tpcc_backend PUBLIC dtx_tatp)

add_executable(tpcc_bench bench.cpp)
target_link_libraries(tpcc_bench PUBLIC dtx_tatp)
