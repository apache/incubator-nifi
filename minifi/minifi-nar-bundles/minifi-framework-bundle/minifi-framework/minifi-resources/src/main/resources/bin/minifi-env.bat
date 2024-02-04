@echo off
rem
rem    Licensed to the Apache Software Foundation (ASF) under one or more
rem    contributor license agreements.  See the NOTICE file distributed with
rem    this work for additional information regarding copyright ownership.
rem    The ASF licenses this file to You under the Apache License, Version 2.0
rem    (the "License"); you may not use this file except in compliance with
rem    the License.  You may obtain a copy of the License at
rem
rem       http://www.apache.org/licenses/LICENSE-2.0
rem
rem    Unless required by applicable law or agreed to in writing, software
rem    distributed under the License is distributed on an "AS IS" BASIS,
rem    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem    See the License for the specific language governing permissions and
rem    limitations under the License.
rem

rem The java implementation to use
rem set JAVA_MINIFI="C:\Java\jdk\"

set "currentDirectory=%~dp0"
set "MINIFI_ROOT="
for %%I in ("%currentDirectory%.") do set "MINIFI_ROOT=%%~dpI"

set MINIFI_PID_DIR=%MINIFI_ROOT%\run
set MINIFI_LOG_DIR=%MINIFI_ROOT%\logs
set MINIFI_APP_LOG_FILE_NAME=minifi-app
set MINIFI_APP_LOG_FILE_EXTENSION=log
set MINIFI_BOOTSTRAP_LOG_FILE_NAME=minifi-bootstrap
set MINIFI_BOOTSTRAP_LOG_FILE_EXTENSION=log

if not defined JAVA_MINIFI if not defined JAVA_HOME (
  echo Please set JAVA_HOME or JAVA_MINIFI
) else if not defined JAVA_MINIFI (
    set JAVA_MINIFI=%JAVA_HOME%
)