cmake_minimum_required(VERSION 3.16)

project(tcp-load-balancer
    VERSION 1.0.0
    DESCRIPTION "Non-blocking Round-Robin TCP load balancer written in modern C++17"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Core library: socket abstraction + load balancing engine
# ---------------------------------------------------------------------------
add_library(tlb_core STATIC
    src/SocketUtils.cpp
    src/LoadBalancer.cpp
)

target_include_directories(tlb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(tlb_core PUBLIC Threads::Threads)

if(WIN32)
    target_link_libraries(tlb_core PUBLIC ws2_32)
    target_compile_definitions(tlb_core PUBLIC
        WIN32_LEAN_AND_MEAN
        NOMINMAX
        _WIN32_WINNT=0x0A00
    )
endif()

if(MSVC)
    target_compile_options(tlb_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(tlb_core PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
endif()

# ---------------------------------------------------------------------------
# CLI demo: load balancer + simulated backends + traffic generator
# ---------------------------------------------------------------------------
add_executable(tcp-load-balancer src/main.cpp)
target_link_libraries(tcp-load-balancer PRIVATE tlb_core)

if(MSVC)
    target_compile_options(tcp-load-balancer PRIVATE /W4 /permissive-)
else()
    target_compile_options(tcp-load-balancer PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
endif()
