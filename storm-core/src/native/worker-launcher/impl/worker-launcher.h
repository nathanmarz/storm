/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME, //2
  INVALID_COMMAND_PROVIDED, //3
  // SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS (NOT USED) 4
  INVALID_NM_ROOT_DIRS = 5,
  SETUID_OPER_FAILED, //6
  UNABLE_TO_EXECUTE_CONTAINER_SCRIPT, //7
  UNABLE_TO_SIGNAL_CONTAINER, //8
  INVALID_CONTAINER_PID, //9
  // ERROR_RESOLVING_FILE_PATH (NOT_USED) 10
  // RELATIVE_PATH_COMPONENTS_IN_FILE_PATH (NOT USED) 11
  // UNABLE_TO_STAT_FILE (NOT USED) 12
  // FILE_NOT_OWNED_BY_ROOT (NOT USED) 13
  // PREPARE_CONTAINER_DIRECTORIES_FAILED (NOT USED) 14
  // INITIALIZE_CONTAINER_FAILED (NOT USED) 15
  // PREPARE_CONTAINER_LOGS_FAILED (NOT USED) 16
  // INVALID_LOG_DIR (NOT USED) 17
  OUT_OF_MEMORY = 18,
  // INITIALIZE_DISTCACHEFILE_FAILED (NOT USED) 19
  INITIALIZE_USER_FAILED = 20,
  UNABLE_TO_BUILD_PATH, //21
  INVALID_CONTAINER_EXEC_PERMISSIONS, //22
  // PREPARE_JOB_LOGS_FAILED (NOT USED) 23
  INVALID_CONFIG_FILE =  24,
  SETSID_OPER_FAILED = 25,
  WRITE_PIDFILE_FAILED = 26
};

#define LAUNCHER_GROUP_KEY "storm.worker-launcher.group"

#define USER_DIR_PATTERN "%s/usercache/%s"
#define NM_APP_DIR_PATTERN USER_DIR_PATTERN "/appcache/%s"
#define CONTAINER_DIR_PATTERN NM_APP_DIR_PATTERN "/%s"
#define CONTAINER_SCRIPT "launch_container.sh"
#define CREDENTIALS_FILENAME "container_tokens"
#define MIN_USERID_KEY "min.user.id"
#define BANNED_USERS_KEY "banned.users"
#define TMP_DIR "tmp"

extern struct passwd *user_detail;

// the log file for messages
extern FILE *LOGFILE;
// the log file for error messages
extern FILE *ERRORFILE;

int setup_stormdist_dir(const char* local_dir);

int exec_as_user(const char * working_dir, const char * args);

// delete a directory (or file) recursively as the user. The directory
// could optionally be relative to the baseDir set of directories (if the same
// directory appears on multiple disk volumes, the disk volumes should be passed
// as the baseDirs). If baseDirs is not specified, then dir_to_be_deleted is 
// assumed as the absolute path
int delete_as_user(const char *user,
                   const char *dir_to_be_deleted,
                   char* const* baseDirs);

// get the executable's filename
char* get_executable();

/**
 * Check the permissions on the worker-launcher to make sure that security is
 * permissible. For this, we need worker-launcher binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have any permissions
 *    * be setuid/setgid
 * @param executable_file the file to check
 * @return -1 on error 0 on success.
 */
int check_executor_permissions(char *executable_file);

/**
 * Function used to signal a container launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the pid.
 * @param user the user to send the signal as.
 * @param pid the process id to send the signal to.
 * @param sig the signal to send.
 * @return an errorcode enum value on error, or 0 on success.
 */
int signal_container_as_user(const char *user, int pid, int sig);

// set the uid and gid of the launcher.  This is used when doing some
// priviledged operations for setting the effective uid and gid.
void set_launcher_uid(uid_t user, gid_t group);

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user);

// set the user
int set_user(const char *user);

// methods to get the directories

char *get_container_launcher_file(const char* work_dir);

int change_user(uid_t user, gid_t group);
