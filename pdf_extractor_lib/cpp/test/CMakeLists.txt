cmake_minimum_required(VERSION 3.29)

project(pdf_extractor_lib_tests)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(APPLE)
    set(CMAKE_OSX_ARCHITECTURES arm64)
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(POPPLER_CPP REQUIRED poppler-cpp)

file(GLOB UNIT_TESTS_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cpp)

add_executable(pdf_extractor_lib_unit_tests
        ${UNIT_TESTS_SRC_FILES}
        ${CMAKE_CURRENT_SOURCE_DIR}/../src/bridge/go/cgo_pdf_extractor.cpp)

target_include_directories(pdf_extractor_lib_unit_tests
        PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/../src
                ${POPPLER_CPP_INCLUDE_DIRS})

target_link_directories(pdf_extractor_lib_unit_tests
        PRIVATE
                ${POPPLER_CPP_LIBRARY_DIRS})

target_link_libraries(pdf_extractor_lib_unit_tests
        PRIVATE
                ${POPPLER_CPP_LIBRARIES})

target_compile_options(pdf_extractor_lib_unit_tests
        PRIVATE
                ${POPPLER_CPP_CFLAGS_OTHER})

enable_testing()
add_test(NAME PDFExtractorLibTest COMMAND pdf_extractor_lib_unit_tests)
