# add_executable(tao_backend backend.cpp)
# target_link_libraries(tao_backend PUBLIC dtx_ycsb)

add_executable(tao_bench bench.cpp)
target_link_libraries(tao_bench PUBLIC dtx_ycsb)
