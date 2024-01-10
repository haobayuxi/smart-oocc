add_executable(tao_backend parse_config.cc backend.cpp)
target_link_libraries(tao_backend PUBLIC dtx_ycsb)

add_executable(tao_bench  parse_config.cc bench.cpp)
target_link_libraries(tao_bench PUBLIC dtx_ycsb)
