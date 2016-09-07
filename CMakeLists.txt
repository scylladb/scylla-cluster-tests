cmake_minimum_required (VERSION 2.8.11)
project (Tests)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})

set(CMAKE_CXX_FLAGS "-std=c++11 -D_FILE_OFFSET_BITS=64")
set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64")

include(ExternalProject)

ExternalProject_Add(
  cpp-driver

  GIT_REPOSITORY "https://github.com/datastax/cpp-driver.git"
  GIT_TAG "2.2.2"

  UPDATE_COMMAND ""
  PATCH_COMMAND ""

  SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/cpp-driver"
  CMAKE_ARGS -DBuildShared=ON -DBuildExamples=OFF -DCMAKE_INSTALL_PREFIX=${PROJECT_SOURCE_DIR}/build/cpp-driver

  TEST_COMMAND ""
)

set(cpp-driver_INCLUDE_DIRS "${PROJECT_SOURCE_DIR}/external/cpp-driver/include")
set(cpp-driver_LIBRARIES "${PROJECT_SOURCE_DIR}/build/cpp-driver/lib64/libcassandra_static.a")
include_directories(${cpp-driver_INCLUDE_DIRS})

# libuv detection took from the cpp-driver
# licence: http://www.apache.org/licenses/LICENSE-2.0
# Copyright 2013-2015 Datastax

set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR} $ENV{LIBUV_ROOT_DIR})
if(NOT WIN32)
  set(_LIBUV_ROOT_PATHS "${_LIBUV_ROOT_PATHS}" "/usr/" "/usr/local/")
endif()
set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS
  HINTS ${_LIBUV_ROOT_HINTS}
  PATHS ${_LIBUV_ROOT_PATHS})

# Ensure libuv was found
find_path(LIBUV_INCLUDE_DIR
  NAMES uv.h
  HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)
find_library(LIBUV_LIBRARY
  NAMES uv libuv
  HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
  LIBUV_LIBRARY
  LIBUV_INCLUDE_DIR)

find_package(Boost 1.53 REQUIRED COMPONENTS program_options)
include_directories(${Boost_INCLUDE_DIR})

add_executable(queries-limits payload/queries-limits.cc)
add_dependencies(queries-limits cpp-driver)

message(status ${cpp-driver_LIBRARIES})

target_link_libraries(queries-limits ${cpp-driver_LIBRARIES})
target_link_libraries(queries-limits ${LIBUV_LIBRARY})
target_link_libraries(queries-limits ${Boost_LIBRARIES} )
