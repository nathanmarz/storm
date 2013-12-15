@echo off

set STORM_HOME=%~dp0
for %%i in (%STORM_HOME%.) do (
  set STORM_HOME=%%~dpi
)
if "%STORM_HOME:~-1%" == "\" (
  set STORM_HOME=%STORM_HOME:~0,-1%
)

if not exist %STORM_HOME%\storm*.jar (
    @echo +================================================================+
    @echo ^|      Error: STORM_HOME is not set correctly                   ^|
    @echo +----------------------------------------------------------------+
    @echo ^| Please set your STORM_HOME variable to the absolute path of   ^|
    @echo ^| the directory that contains the storm distribution            ^|
    @echo +================================================================+
    exit /b 1
)

set STORM_BIN_DIR=%STORM_HOME%\bin
set STORM_SBIN_DIR=%STORM_HOME%\sbin

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
set CLASSPATH=!CLASSPATH!;%JAVA_HOME%\lib\tools.jar

@rem
@rem add libs to CLASSPATH
@rem

set CLASSPATH=!CLASSPATH!;%STORM_HOME%\lib\*

@rem
@rem add sbin to CLASSPATH
@rem

set CLASSPATH=!CLASSPATH!;%STORM_HOME%\sbin\*

if not defined STORM_LOG_DIR (
  set STORM_LOG_DIR=%STORM_HOME%\logs
)

if not defined STORM_LOGFILE (
  set STORM_LOGFILE=storm.log
)

if not defined STORM_ROOT_LOGGER (
  set STORM_ROOT_LOGGER=INFO,console,DRFA
)

set STORM_OPTS=-Dstorm.home=%STORM_HOME% -Djava.library.path=sbin -Dlog4j.configuration=storm.log.properties
set STORM_OPTS=%STORM_OPTS% -Dstorm.log.dir=%STORM_LOG_DIR%
set STORM_OPTS=%STORM_OPTS% -Dstorm.root.logger=%STORM_ROOT_LOGGER%

if not defined STORM_SERVER_OPTS (
  set STORM_SERVER_OPTS=-server
)

if not defined STORM_CLIENT_OPTS (
  set STORM_CLIENT_OPTS=-client
)

:eof
