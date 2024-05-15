file(GLOB SRC ./common/*.c)
add_library(common STATIC ${SRC})
target_link_libraries(common pthread)

add_executable(client client.c ${SRC})
target_link_libraries(client common pthread fabric)
