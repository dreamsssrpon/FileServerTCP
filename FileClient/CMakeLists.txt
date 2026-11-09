cmake_minimum_required(VERSION 3.16)

project(client)

#find_package(Boost REQUIRED)#Required if not found files warning
find_package(Boost CONFIG REQUIRED COMPONENTS thread filesystem process)#
add_executable(client main.cpp Networker.h)

target_link_libraries(client PRIVATE Boost::thread Boost::filesystem Boost::process)#cmake --build build