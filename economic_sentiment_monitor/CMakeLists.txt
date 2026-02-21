# Top-level CMake build for ESM C core
cmake_minimum_required(VERSION 3.20)
project(esm VERSION 0.1.0 LANGUAGES C)
set(CMAKE_C_STANDARD 11)
add_subdirectory(core)
add_subdirectory(gui)