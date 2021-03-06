# -*- mode: CMake; coding: utf-8; -*-
CMAKE_MINIMUM_REQUIRED(VERSION 2.8.4)
PROJECT(sagittarius-dbd-sqlite3)

IF (CYGWIN)
  # assume using default one
  SET(SQLITE3_LIBRARY "/bin/cygsqlite3-0.dll")
ELSE()
  # i'm not sure if this works on other platform.
  SET(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
  FIND_PACKAGE(Sqlite3 REQUIRED)
  IF (NOT SQLITE3_FOUND)
    MESSAGE(FATAL_ERROR "sqlite3 is required")
  ENDIF()
  SET(SQLITE3_LIBRARY ${SQLITE3_LIBRARY_RELEASE})
ENDIF()

# uninstall target
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/sqlite3.scm.in"
  "${CMAKE_CURRENT_BINARY_DIR}/sqlite3.scm"
  IMMEDIATE @ONLY)


SET(SAGITTARIUS_CONFIG  "sagittarius-config")
SET(SAGITTARIUS_PACKAGE "sagittarius-package")

EXECUTE_PROCESS(
  COMMAND ${SAGITTARIUS_CONFIG} --pkglibdir
  OUTPUT_VARIABLE SAGITTARIUS_SITE_LIB_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/sqlite3.scm
  DESTINATION ${SAGITTARIUS_SITE_LIB_DIR})
INSTALL(FILES dbd/sqlite3.scm
  DESTINATION ${SAGITTARIUS_SITE_LIB_DIR}/dbd)

# uninstall target
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
