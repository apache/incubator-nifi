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

call %~sdp0\minifi-env.bat

setlocal enabledelayedexpansion

set "arg1="
set "arg2="
set "arg3="
set "errorFlag=0"

set "argCount=0"
for %%A in (%*) do (
    set /a "argCount+=1"

    if !argCount! equ 1 set "arg1=%%~A"
    if !argCount! equ 2 set "arg2=%%~A"
    if !argCount! equ 3 set "arg3=%%~A"

    if !argCount! geq 4 (
        set "errorFlag=tooManyArguments"
        goto :error
    )
	
	if "!arg1:~0,11!" equ "serviceName" (
		set "serviceName=!arg1:~12!"
	) else ( 
		if "!arg2:~0,11!" equ "serviceName" (
			set "serviceName=!arg2:~12!"
		) else ( 
			if "!arg3:~0,11!" equ "serviceName" (
				set "serviceName=!arg3:~12!" 
			)
		)
	)
	
	if "!arg1:~0,11!" equ "serviceUser" (
		set "serviceUser=!arg1:~12!"
	) else ( 
		if "!arg2:~0,11!" equ "serviceUser" (
			set "serviceUser=!arg2:~12!"
		) else ( 
			if "!arg3:~0,11!" equ "serviceUser" (
				set "serviceUser=!arg3:~12!" 
			)
		)
	)
	
	if not "!serviceUser!"=="" (
		if "!arg1:~0,19!" equ "serviceUserPassword" (
			set "serviceUserPassword=!arg1:~20!"
		) else ( 
			if "!arg2:~0,19!" equ "serviceUserPassword" (
				set "serviceUserPassword=!arg2:~20!"
			) else ( 
				if "!arg3:~0,19!" equ "serviceUserPassword" (
					set "serviceUserPassword=!arg3:~20!" 
				)
			)
		)
	)
)

net user %serviceUser% >nul 2>&1
if not !errorlevel! == 0 (
	set "errorFlag=userNotExist"
	goto :error
)

set CONF_DIR=%MINIFI_ROOT%\conf
set BOOTSTRAP_CONF_FILE=%CONF_DIR%\bootstrap.conf
set SRV_BIN=%cd%\minifi.exe
set SVC_NAME=%serviceName%
set SVC_DISPLAY="Apache MiNiFi"
set SVC_DESCRIPTION="Apache MiNiFi"
set JVM=auto
set PR_JVMMS=12
set PR_JVMMX=24
set PR_JVMSS=4000
set START_MODE=jvm
set STOP_MODE=jvm
set STOP_TIMEOUT=10
set STARTUP=auto
set JAVA_ARGS=-Dorg.apache.nifi.minifi.bootstrap.config.log.dir="%MINIFI_LOG_DIR%";-Dorg.apache.nifi.minifi.bootstrap.config.pid.dir="%MINIFI_PID_DIR%";-Dorg.apache.nifi.minifi.bootstrap.config.file="%BOOTSTRAP_CONF_FILE%";-Dorg.apache.nifi.minifi.bootstrap.config.log.app.file.name="%MINIFI_APP_LOG_FILE_NAME%";-Dorg.apache.nifi.minifi.bootstrap.config.log.app.file.extension="%MINIFI_APP_LOG_FILE_EXTENSION%";-Dorg.apache.nifi.minifi.bootstrap.config.log.bootstrap.file.name="%MINIFI_BOOTSTRAP_LOG_FILE_NAME%";-Dorg.apache.nifi.minifi.bootstrap.config.log.bootstrap.file.extension="%MINIFI_BOOTSTRAP_LOG_FILE_EXTENSION%"
set START_CLASS=org.apache.nifi.minifi.bootstrap.WindowsService
set START_METHOD=start
set STOP_CLASS=org.apache.nifi.minifi.bootstrap.WindowsService
set STOP_METHOD=stop
set CLASS_PATH="%CONF_DIR%";"%MINIFI_ROOT%lib\*";"%MINIFI_ROOT%lib\bootstrap\*"
set LOG_PATH="%MINIFI_ROOT%\logs"
set LOG_PREFIX=minifi.log

"%SRV_BIN%" //IS//%SVC_NAME% ^
--DisplayName=%SVC_DISPLAY% ^
--Description=%SVC_DESCRIPTION% ^
--Install="%SRV_BIN%" ^
--Jvm="%JVM%" ^
--JvmMs="%PR_JVMMS%" ^
--JvmMx="%PR_JVMMX%" ^
--JvmSs="%PR_JVMSS%" ^
--StartMode="%START_MODE%" ^
--StopMode="%STOP_MODE%" ^
--Startup="%STARTUP%" ^
--StartClass="%START_CLASS%" ^
--StopClass="%STOP_CLASS%" ^
--StartParams="%START_PARAMS%" ^
--StopParams="%STOP_PARAMS%" ^
--StartMethod="%START_METHOD%" ^
--StopMethod="%STOP_METHOD%" ^
--StopTimeout="%STOP_TIMEOUT%" ^
--Classpath="%CLASS_PATH%" ^
--JvmOptions9="%JAVA_ARGS%" ^
--LogLevel=ERROR ^
--LogPath="%LOG_PATH%" ^
--LogPrefix="%LOG_PREFIX%" ^
--StdOutput=auto ^
--StdError=auto ^
--ServiceUser="%serviceUser%" ^
--ServicePassword="%serviceUserPassword%"

:error
if !errorFlag! equ tooManyArguments (
    echo Error: Too many arguments!
) 
if !errorFlag! equ userNotExist (
    echo Error: User !serviceUser! does not exist. Please create it and re-run the installation script.
)

endlocal