@echo off

@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.


set STORM_HOME=%~dp0
for %%i in (%STORM_HOME%.) do (
  set STORM_HOME=%%~dpi
)
if "%STORM_HOME:~-1%" == "\" (
  set STORM_HOME=%STORM_HOME:~0,-1%
)

if not exist %STORM_HOME%\lib\storm*.jar (
    @echo +================================================================+
    @echo ^|      Error: STORM_HOME is not set correctly                   ^|
    @echo +----------------------------------------------------------------+
    @echo ^| Please set your STORM_HOME variable to the absolute path of   ^|
    @echo ^| the directory that contains the storm distribution      ^|
    @echo +================================================================+
    exit /b 1
)

set STORM_BIN_DIR=%STORM_HOME%\bin

if not defined STORM_CONF_DIR (
  set STORM_CONF_DIR=%STORM_HOME%\conf
)

@rem
@rem setup java environment variables
@rem

if not defined JAVA_HOME (
  set JAVA_HOME=c:\apps\java\openjdk7
)

if not exist %JAVA_HOME%\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  goto :eof
)

set JAVA=%JAVA_HOME%\bin\java
set JAVA_HEAP_MAX=-Xmx1024m

@rem
@rem check envvars which might override default args
@rem

if defined STORM_HEAPSIZE (
  set JAVA_HEAP_MAX=-Xmx%STORM_HEAPSIZE%m
)

@rem
@rem CLASSPATH initially contains %STORM_CONF_DIR%
@rem

set CLASSPATH=%STORM_HOME%\*;%STORM_CONF_DIR%
set CLASSPATH=%CLASSPATH%;%JAVA_HOME%\lib\tools.jar

@rem
@rem add libs to CLASSPATH
@rem

set CLASSPATH=!CLASSPATH!;%STORM_HOME%\lib\*

if not defined STORM_LOG_DIR (
  set STORM_LOG_DIR=%STORM_HOME%\logs
)

if not defined STORM_LOGBACK_CONFIGURATION_FILE (
  set STORM_LOGBACK_CONFIGURATION_FILE=%STORM_HOME%\logback\cluster.xml
)
%JAVA% -client -Dstorm.options= -Dstorm.conf.file= -cp %CLASSPATH% backtype.storm.command.config_value java.library.path > temp.txt

FOR /F "delims=" %%i in (temp.txt) do (
    FOR /F "tokens=1,* delims= " %%a in ("%%i") do (
	 if %%a == VALUE: (
	   set JAVA_LIBRARY_PATH=%%b
	   goto :storm_opts)
  )
)


:storm_opts
 set STORM_OPTS=-Dstorm.options= -Dstorm.home=%STORM_HOME% -Djava.library.path=%JAVA_LIBRARY_PATH%
 set STORM_OPTS=%STORM_OPTS% -Dlogback.configurationFile=%STORM_LOGBACK_CONFIGURATION_FILE%
 set STORM_OPTS=%STORM_OPTS% -Dstorm.log.dir=%STORM_LOG_DIR%
 del /F temp.txt


if not defined STORM_SERVER_OPTS (
  set STORM_SERVER_OPTS=-server
)

if not defined STORM_CLIENT_OPTS (
  set STORM_CLIENT_OPTS=-client
)

:eof
