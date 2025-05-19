cmake_minimum_required(VERSION 3.10)

project(FrequencyCounter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -O2)

find_package(Threads REQUIRED)

add_executable(frequency_counter frequency_counter.cpp)

target_link_libraries(frequency_counter PRIVATE Threads::Threads)