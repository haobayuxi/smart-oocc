# add_library(dtx_smallbank dtx.cpp dslr.cpp drtm.cpp)
# target_compile_definitions(dtx_smallbank PUBLIC CONFIG_ITEM_SIZE=40)
# target_link_libraries(dtx_smallbank PUBLIC smart)

# add_library(dtx_tatp dtx.cpp drtm.cpp dslr.cpp)
# target_compile_definitions(dtx_tatp PUBLIC CONFIG_ITEM_SIZE=40)
# target_link_libraries(dtx_tatp PUBLIC smart)

add_library(dtx_ycsb dtx.cpp dslr.cpp drtm.cpp)
target_compile_definitions(dtx_ycsb PUBLIC CONFIG_ITEM_SIZE=8)
target_link_libraries(dtx_ycsb PUBLIC smart)


# add_library(dtx_tao dtx.cpp dslr.cpp drtm.cpp)
# target_compile_definitions(dtx_tao PUBLIC CONFIG_ITEM_SIZE=150)
# target_link_libraries(dtx_tao PUBLIC smart)

add_library(dtx_tpcc dtx.cpp drtm.cpp dslr.cpp)
target_compile_definitions(dtx_tpcc PUBLIC CONFIG_ITEM_SIZE=664)
target_link_libraries(dtx_tpcc PUBLIC smart)

# add_subdirectory(smallbank)
# add_subdirectory(tatp)
add_subdirectory(ycsb)
add_subdirectory(tpcc)
# add_subdirectory(taobench)

