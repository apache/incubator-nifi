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

rem Use JAVA_HOME if it's set; otherwise, just use java

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
set JAVA_EXE=%JAVA_HOME%\bin\java.exe
set TOOLS_JAR=%JAVA_HOME%\lib\tools.jar
goto envNifi

:noJavaHome
echo The JAVA_HOME environment variable is not defined correctly.
echo Instead the PATH will be used to find the java executable.
echo.
rem set JAVA_EXE=java
for /f %%j in ("java.exe") do (
    set JAVA_HOME=%%~dp$PATH:j
)

if %JAVA_HOME%.==. (
    @echo java.exe not found
    goto:eof
)
set JAVA_EXE=%JAVA_HOME%\bin\java.exe
set TOOLS_JAR=%JAVA_HOME%\lib\tools.jar
goto envNifi

:envNifi
set NIFI_ROOT=%~dp0..\
pushd "%NIFI_ROOT%"
set LIB_DIR=lib\bootstrap
set CONF_DIR=conf

set BOOTSTRAP_CONF_FILE=%CONF_DIR%\bootstrap.conf
set JAVA_ARGS=-Dorg.apache.nifi.bootstrap.config.file=%BOOTSTRAP_CONF_FILE%

set JAVA_PARAMS=-cp %TOOLS_JAR%:%LIB_DIR%\* -Xms12m -Xmx24m %JAVA_ARGS% org.apache.nifi.bootstrap.RunNiFi
set BOOTSTRAP_ACTION=env

cmd.exe /C "%JAVA_EXE%" %JAVA_PARAMS% %BOOTSTRAP_ACTION%

popd
