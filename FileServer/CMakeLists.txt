cmake_minimum_required(VERSION 3.16)

project(SER)

#find_package(Boost REQUIRED)#Required if not found files warning
find_package(Boost CONFIG REQUIRED COMPONENTS thread filesystem)#
add_executable(SER main.cpp Networker.h)

target_link_libraries(SER PRIVATE Boost::thread Boost::filesystem)#cmake --build build