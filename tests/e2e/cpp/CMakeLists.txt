cmake_minimum_required(VERSION 3.14)
project(capnpc_zig_e2e_cpp CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(CapnProto REQUIRED)

# Schema directory
set(SCHEMA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../schemas")

# We compile schemas manually so we can control import paths and output location.
# The capnp_generate_cpp() macro doesn't always handle out-of-tree schemas well.
set(SCHEMA_FILES
  game_types.capnp
  game_world.capnp
  chat.capnp
  inventory.capnp
  matchmaking.capnp
)

set(CAPNP_SRCS)
set(CAPNP_HDRS)

foreach(SCHEMA ${SCHEMA_FILES})
  get_filename_component(SCHEMA_BASE ${SCHEMA} NAME_WE)
  set(SRC "${CMAKE_CURRENT_BINARY_DIR}/${SCHEMA}.c++")
  set(HDR "${CMAKE_CURRENT_BINARY_DIR}/${SCHEMA}.h")

  add_custom_command(
    OUTPUT ${SRC} ${HDR}
    COMMAND capnp compile
      -o c++:${CMAKE_CURRENT_BINARY_DIR}
      --src-prefix=${SCHEMA_DIR}
      -I ${SCHEMA_DIR}
      ${SCHEMA_DIR}/${SCHEMA}
    DEPENDS ${SCHEMA_DIR}/${SCHEMA}
    COMMENT "Compiling ${SCHEMA}"
  )

  list(APPEND CAPNP_SRCS ${SRC})
  list(APPEND CAPNP_HDRS ${HDR})
endforeach()

# Server executable
add_executable(e2e_server
  main_server.cpp
  ${CAPNP_SRCS}
)
target_link_libraries(e2e_server CapnProto::capnp-rpc)
target_include_directories(e2e_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# Client executable
add_executable(e2e_client
  main_client.cpp
  ${CAPNP_SRCS}
)
target_link_libraries(e2e_client CapnProto::capnp-rpc)
target_include_directories(e2e_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
