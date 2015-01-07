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
#include "configuration.h"
#include "worker-launcher.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>

#define TEST_ROOT "/tmp/test-worker-launcher"
#define DONT_TOUCH_FILE "dont-touch-me"
#define NM_LOCAL_DIRS       TEST_ROOT "/local-1," TEST_ROOT "/local-2," \
               TEST_ROOT "/local-3," TEST_ROOT "/local-4," TEST_ROOT "/local-5"
#define NM_LOG_DIRS         TEST_ROOT "/logdir_1," TEST_ROOT "/logdir_2," \
                            TEST_ROOT "/logdir_3," TEST_ROOT "/logdir_4"
#define ARRAY_SIZE 1000

static char* username = NULL;
static char* local_dirs = NULL;
static char* log_dirs = NULL;

/**
 * Run the command using the effective user id.
 * It can't use system, since bash seems to copy the real user id into the
 * effective id.
 */
void run(const char *cmd) {
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork - %s\n", strerror(errno));
  } else if (child == 0) {
    char *cmd_copy = strdup(cmd);
    char *ptr;
    int words = 1;
    for(ptr = strchr(cmd_copy, ' ');  ptr; ptr = strchr(ptr+1, ' ')) {
      words += 1;
    }
    char **argv = malloc(sizeof(char *) * (words + 1));
    ptr = strtok(cmd_copy, " ");
    int i = 0;
    argv[i++] = ptr;
    while (ptr != NULL) {
      ptr = strtok(NULL, " ");
      argv[i++] = ptr;
    }
    if (execvp(argv[0], argv) != 0) {
      printf("FAIL: exec failed in child %s - %s\n", cmd, strerror(errno));
      exit(42);
    }
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) <= 0) {
      printf("FAIL: failed waiting for child process %s pid %d - %s\n", 
	     cmd, child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: process %s pid %d did not exit\n", cmd, child);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: process %s pid %d exited with error status %d\n", cmd, 
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

int write_config_file(char *file_name) {
  FILE *file;
  file = fopen(file_name, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", file_name);
    return EXIT_FAILURE;
  }
  fprintf(file, "banned.users=bannedUser\n");
  fprintf(file, "min.user.id=%d\n",getuid());
  fclose(file);
  return 0;
}

void create_nm_roots(char ** nm_roots) {
  char** nm_root;
  for(nm_root=nm_roots; *nm_root != NULL; ++nm_root) {
    if (mkdir(*nm_root, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", *nm_root,
             strerror(errno));
      exit(1);
    }
    char buffer[100000];
    sprintf(buffer, "%s/usercache", *nm_root);
    if (mkdir(buffer, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", buffer,
             strerror(errno));
      exit(1);
    }
  }
}

void test_get_container_launcher_file() {
  char *expected_file = ("/tmp/launch_container.sh");
  char *app_dir = "/tmp";
  char *container_file =  get_container_launcher_file(app_dir);
  if (strcmp(container_file, expected_file) != 0) {
    printf("failure to match expected container file %s vs %s\n", container_file,
           expected_file);
    exit(1);
  }
  free(container_file);
}

void test_check_user() {
  printf("\nTesting test_check_user\n");
  struct passwd *user = check_user(username);
  if (user == NULL) {
    printf("FAIL: failed check for user %s\n", username);
    exit(1);
  }
  free(user);
  if (check_user("lp") != NULL) {
    printf("FAIL: failed check for system user lp\n");
    exit(1);
  }
  if (check_user("root") != NULL) {
    printf("FAIL: failed check for system user root\n");
    exit(1);
  }
}

void test_check_configuration_permissions() {
  printf("\nTesting check_configuration_permissions\n");
  if (check_configuration_permissions("/etc/passwd") != 0) {
    printf("FAIL: failed permission check on /etc/passwd\n");
    exit(1);
  }
  if (check_configuration_permissions(TEST_ROOT) == 0) {
    printf("FAIL: failed permission check on %s\n", TEST_ROOT);
    exit(1);
  }
}

void run_test_in_child(const char* test_name, void (*func)()) {
  printf("\nRunning test %s in child process\n", test_name);
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    func();
    exit(0);
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid %d failed - %s\n", child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: child %d didn't exit - %d\n", child, status);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: child %d exited with bad status %d\n",
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

void test_signal_container() {
  printf("\nTesting signal_container\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  } else {
    printf("Child container launched as %d\n", child);
    if (signal_container_as_user(username, child, SIGQUIT) != 0) {
      exit(1);
    }
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid failed - %s\n", strerror(errno));
      exit(1);
    }
    if (!WIFSIGNALED(status)) {
      printf("FAIL: child wasn't signalled - %d\n", status);
      exit(1);
    }
    if (WTERMSIG(status) != SIGQUIT) {
      printf("FAIL: child was killed with %d instead of %d\n", 
	     WTERMSIG(status), SIGQUIT);
      exit(1);
    }
  }
}

void test_signal_container_group() {
  printf("\nTesting group signal_container\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    setpgrp();
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  }
  printf("Child container launched as %d\n", child);
  if (signal_container_as_user(username, child, SIGKILL) != 0) {
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) == -1) {
    printf("FAIL: waitpid failed - %s\n", strerror(errno));
    exit(1);
  }
  if (!WIFSIGNALED(status)) {
    printf("FAIL: child wasn't signalled - %d\n", status);
    exit(1);
  }
  if (WTERMSIG(status) != SIGKILL) {
    printf("FAIL: child was killed with %d instead of %d\n", 
	   WTERMSIG(status), SIGKILL);
    exit(1);
  }
}

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm) {
  char *cmd = malloc(10 + strlen(path));
  int ret = 0;
  sprintf(cmd, "mkdir -p %s", path);
  ret = system(cmd);
  free(cmd);
  return ret;
}

int main(int argc, char **argv) {
  LOGFILE = stdout;
  ERRORFILE = stderr;
  int my_username = 0;

  // clean up any junk from previous run
  system("chmod -R u=rwx " TEST_ROOT "; rm -fr " TEST_ROOT);
  
  if (mkdirs(TEST_ROOT "/logs/userlogs", 0755) != 0) {
    exit(1);
  }
  
  if (write_config_file(TEST_ROOT "/test.cfg") != 0) {
    exit(1);
  }
  read_config(TEST_ROOT "/test.cfg");

  local_dirs = (char *) malloc (sizeof(char) * ARRAY_SIZE);
  strcpy(local_dirs, NM_LOCAL_DIRS);
  log_dirs = (char *) malloc (sizeof(char) * ARRAY_SIZE);
  strcpy(log_dirs, NM_LOG_DIRS);

  create_nm_roots(extract_values(local_dirs));

  if (getuid() == 0 && argc == 2) {
    username = argv[1];
  } else {
    username = strdup(getpwuid(getuid())->pw_name);
    my_username = 1;
  }
  set_launcher_uid(geteuid(), getegid());

  if (set_user(username)) {
    exit(1);
  }

  printf("\nStarting tests\n");

  printf("\nTesting get_container_launcher_file()\n");
  test_get_container_launcher_file();

  printf("\nTesting check_configuration_permissions()\n");
  test_check_configuration_permissions();

  printf("\nTesting check_user()\n");
  test_check_user();

  // the tests that change user need to be run in a subshell, so that
  // when they change user they don't give up our privs
  run_test_in_child("test_signal_container", test_signal_container);
  run_test_in_child("test_signal_container_group", test_signal_container_group);

  seteuid(0);
  run("rm -fr " TEST_ROOT);
  printf("\nFinished tests\n");

  if (my_username) {
    free(username);
  }
  free_configurations();
  return 0;
}
