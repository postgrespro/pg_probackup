cmake_minimum_required(VERSION 3.11)

add_executable(array array.c)
target_link_libraries(array fu_utils)

add_executable(bsearch bsearch.c)
target_link_libraries(bsearch fu_utils)

add_executable(fuprintf fuprintf.c)
target_link_libraries(fuprintf fu_utils)

add_executable(sort sort.c)
target_link_libraries(sort fu_utils)

add_executable(sort_p sort_p.c)
target_link_libraries(sort_p fu_utils)

add_executable(obj1 obj1.c)
target_link_libraries(obj1 fu_utils)

enable_testing()

add_test(NAME array COMMAND array)
add_test(NAME bsearch COMMAND bsearch)
add_test(NAME fuprintf COMMAND fuprintf)
add_test(NAME sort COMMAND sort)
add_test(NAME sort_p COMMAND sort_p)
add_test(NAME obj1 COMMAND obj1)
