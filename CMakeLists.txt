cmake_minimum_required(VERSION 3.10)
project(CBRedis)

#set(CMAKE_CXX_FLAGS -static-libstdc++)

set(CBROTHER_ROOT XXXX/cbrother)
set(REDIS_INCLUDE_DIR XXXX/hiredis-master)
set(REDIS_BINARY_DIR XXXX/hiredis-master/build/Release)

include_directories(
	${CBROTHER_ROOT}/include
	${REDIS_INCLUDE_DIR}
)

link_directories(
	${REDIS_BINARY_DIR}
   )

add_library(CBRedis SHARED
        ./CBRedis.cpp)

target_link_libraries(${PROJECT_NAME} hiredis_static)

