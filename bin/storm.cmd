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

@rem The storm command script
@rem
@rem Environment Variables
@rem
@rem   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
@rem
@rem   STORM_CLASSPATH Extra Java CLASSPATH entries.
@rem
@rem   STORM_HEAPSIZE  The maximum amount of heap to use, in MB.
@rem                    Default is 1000.
@rem
@rem   STORM_OPTS      Extra Java runtime options.
@rem
@rem   STORM_CONF_DIR  Alternate conf dir. Default is ${STORM_HOME}/conf.
@rem
@rem   STORM_ROOT_LOGGER The root appender. Default is INFO,console
@rem

:main
  setlocal enabledelayedexpansion

  call %~dp0storm-config.cmd

  set storm-command=%1
  if not defined storm-command (
      goto print_usage
  )

  call :make_command_arguments %*

  set shellcommands=classpath help version
  for %%i in ( %shellcommands% ) do (
    if %storm-command% == %%i set shellcommand=true
  )
  if defined shellcommand (
    call :%storm-command% %*
    goto :eof
  )

  set corecommands=activate deactivate dev-zookeeper drpc kill list nimbus rebalance repl shell supervisor ui
  for %%i in ( %corecommands% ) do (
    if %storm-command% == %%i set corecommand=true  
  )
  if defined corecommand (
    call :%storm-command% %storm-command-arguments%
  ) else (
    set CLASS=%storm-command%
  )

  if %storm-command% == jar (
    set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS% -Dstorm.jar=%2
    set CLASSPATH=%CLASSPATH%;%2
    set CLASS=%3
    set storm-command-arguments=%4 %5 %6 %7 %8 %9
  )
  
  if not defined STORM_LOG_FILE (
    set STORM_LOG_FILE=-Dlogfile.name=%storm-command%.log
  )

  if defined STORM_DEBUG ( 
    %JAVA% %JAVA_HEAP_MAX% %STORM_OPTS% %STORM_LOG_FILE% %CLASS% %storm-command-arguments%
  )
  set path=%PATH%;%STORM_BIN_DIR%;%STORM_SBIN_DIR%
  call %JAVA% %JAVA_HEAP_MAX% %STORM_OPTS% %STORM_LOG_FILE% %CLASS% %storm-command-arguments%
  goto :eof


:activate
  set CLASS=backtype.storm.command.activate
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:classpath
  echo %CLASSPATH% 
  goto :eof

:deactivate
  set CLASS=backtype.storm.command.deactivate
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:dev-zookeeper
  set CLASS=backtype.storm.command.dev_zookeeper
  set STORM_OPTS=%STORM_SERVER_OPTS% %STORM_OPTS%
  goto :eof

:drpc
  set CLASS=backtype.storm.daemon.drpc
  set STORM_OPTS=%STORM_SERVER_OPTS% %STORM_OPTS%
  goto :eof

:help
  call :print_usage
  goto :eof

:kill
  set CLASS=backtype.storm.command.kill_topology
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:list
  set CLASS=backtype.storm.command.list
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:nimbus
  set CLASS=backtype.storm.daemon.nimbus
  set STORM_OPTS=%STORM_SERVER_OPTS% %STORM_OPTS%
  goto :eof

:rebalance
  set CLASS=backtype.storm.command.rebalance
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:repl
  set CLASS=clojure.main
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS%
  goto :eof

:shell
  set CLASS=backtype.storm.command.shell_submission
  set STORM_OPTS=%STORM_CLIENT_OPTS% %STORM_OPTS% 
  goto :eof
  
:supervisor
  set CLASS=backtype.storm.daemon.supervisor
  set STORM_OPTS=%STORM_SERVER_OPTS% %STORM_OPTS%
  goto :eof

:ui
  set CLASS=backtype.storm.ui.core
  set CLASSPATH=%CLASSPATH%;%STORM_HOME%
  set STORM_OPTS=%STORM_SERVER_OPTS% %STORM_OPTS%
  goto :eof

:version
  type RELEASE
  goto :eof

:make_command_arguments
  if "%2" == "" goto :eof
  set _count=0
  set _shift=1
  for %%i in (%*) do (
    set /a _count=!_count!+1
    if !_count! GTR %_shift% ( 
	if not defined _arguments (
	  set _arguments=%%i
	) else (
          set _arguments=!_arguments! %%i
	)
    )
  )
  set storm-command-arguments=%_arguments%
  goto :eof

:print_usage
  @echo Usage: storm COMMAND
  @echo where COMMAND is one of:
  @echo   activate             activates the specified topology's spouts
  @echo   classpath            prints the classpath used by the storm client when running commands
  @echo   deactivate           deactivates the specified topology's spouts
  @echo   dev-zookeeper        launches a fresh dev/test Zookeeper server
  @echo   drpc                 launches a DRPC daemon
  @echo   help
  @echo   jar ^<jar^>            run a jar file
  @echo   kill                 kills the topology with the name topology-name
  @echo   list                 list the running topologies and their statuses
  @echo   nimbus               launches the nimbus daemon
  @echo   rebalance            redistribute or change the parallelism of a running topology
  @echo   repl                 opens up a Clojure REPL
  @echo   shell                storm shell
  @echo   supervisor           launches the supervisor daemon
  @echo   ui                   launches the UI daemon
  @echo   version              print the version
  @echo.
  @echo  or
  @echo   CLASSNAME            run the class named CLASSNAME
  @echo Most commands print help when invoked w/o parameters.

endlocal
