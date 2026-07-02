message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CPP_ROOT}/build/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CPP_ROOT}/build/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CPP_ROOT}/build/bin)

set(CPP_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CPP_BRIDGE_GO_DIR ${CPP_SRC_DIR}/bridge/go)

add_library(go_pdf_extractor_lib STATIC)

target_sources(go_pdf_extractor_lib
        PRIVATE
                ${CPP_BRIDGE_GO_DIR}/cgo_pdf_extractor.cpp)

target_include_directories(go_pdf_extractor_lib
        PUBLIC
                ${CPP_SRC_DIR}
                ${POPPLER_CPP_INCLUDE_DIRS})

target_link_directories(go_pdf_extractor_lib
        PUBLIC
                ${POPPLER_CPP_LIBRARY_DIRS})

target_link_libraries(go_pdf_extractor_lib
        PUBLIC
                ${POPPLER_CPP_LIBRARIES})

target_compile_options(go_pdf_extractor_lib
        PRIVATE
                ${POPPLER_CPP_CFLAGS_OTHER})

