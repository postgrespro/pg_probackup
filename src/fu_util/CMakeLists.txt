cmake_minimum_required(VERSION 3.16)
project(fu_utils VERSION 0.1 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_EXTENSIONS true)

include(CheckCSourceCompiles)

add_library(fu_utils impl/ft_impl.c impl/fo_impl.c)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(fu_utils PRIVATE Threads::Threads)

# Detect for installed beautiful https://github.com/ianlancetaylor/libbacktrace
include_directories(.)
if(NOT CMAKE_C_COMPILER MATCHES tcc)
    find_library(LIBBACKTRACE backtrace)
    if(LIBBACKTRACE)
        set(CMAKE_REQUIRED_LIBRARIES backtrace)
        check_c_source_compiles("
            #include <backtrace.h>
            int main(void) {
                struct backtrace_state *st = backtrace_create_state(NULL, 0, NULL, NULL);
                return 0;
            }
        " HAVE_LIBBACKTRACE)
        if (HAVE_LIBBACKTRACE)
            target_compile_definitions(fu_utils PRIVATE HAVE_LIBBACKTRACE)
        endif()
    endif()
endif()

configure_file(fu_utils_cfg.h.in fu_utils_cfg.h)
target_include_directories(fu_utils INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(fu_utils PRIVATE "${PROJECT_BINARY_DIR}")
target_link_libraries(fu_utils PUBLIC backtrace)

install(TARGETS fu_utils DESTINATION lib)
install(FILES fm_util.h ft_util.h fo_obj.h
        ft_sort.inc.h ft_ss_examples.h ft_search.inc.h ft_array.inc.h
        ft_ar_examples.h "${PROJECT_BINARY_DIR}/fu_utils_cfg.h"
        DESTINATION include/fu_utils)
install(FILES impl/ft_impl.h impl/fo_impl.h DESTINATION include/fu_utils/impl)

add_subdirectory(test)