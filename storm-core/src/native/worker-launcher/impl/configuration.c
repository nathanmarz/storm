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

// ensure we get the posix version of dirname by including this first
#include <libgen.h> 

#include "configuration.h"
#include "worker-launcher.h"

#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MAX_SIZE 10

struct confentry {
  const char *key;
  const char *value;
};

struct configuration {
  int size;
  struct confentry **confdetails;
};

struct configuration config={.size=0, .confdetails=NULL};

//clean up method for freeing configuration
void free_configurations() {
  int i = 0;
  for (i = 0; i < config.size; i++) {
    if (config.confdetails[i]->key != NULL) {
      free((void *)config.confdetails[i]->key);
      config.confdetails[i]->key = NULL;
    }
    if (config.confdetails[i]->value != NULL) {
      free((void *)config.confdetails[i]->value);
      config.confdetails[i]->value = NULL;
    }
    free(config.confdetails[i]);
    config.confdetails[i] = NULL;
  }
  if (config.size > 0) {
    free(config.confdetails);
    config.confdetails = NULL;
  }
  config.size = 0;
}

/**
 * Is the file/directory only writable by root.
 * Returns 1 if true
 */
static int is_only_root_writable(const char *file) {
  struct stat file_stat;
  if (stat(file, &file_stat) != 0) {
    fprintf(ERRORFILE, "Can't stat file %s - %s\n", file, strerror(errno));
    return 0;
  }
  if (file_stat.st_uid != 0) {
    fprintf(ERRORFILE, "File %s must be owned by root, but is owned by %d\n",
            file, file_stat.st_uid);
    return 0;
  }
  if ((file_stat.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
    fprintf(ERRORFILE, 
	    "File %s must not be world or group writable, but is %03o\n",
	    file, file_stat.st_mode & (~S_IFMT));
    return 0;
  }
  return 1;
}

/**
 * Ensure that the configuration file and all of the containing directories
 * are only writable by root. Otherwise, an attacker can change the 
 * configuration and potentially cause damage.
 * returns 0 if permissions are ok
 */
int check_configuration_permissions(const char* file_name) {
  // copy the input so that we can modify it with dirname
  char* dir = strndup(file_name, PATH_MAX);
  char* buffer = dir;
  do {
    if (!is_only_root_writable(dir)) {
      free(buffer);
      buffer = NULL;
      return -1;
    }
    dir = dirname(dir);
  } while (strcmp(dir, "/") != 0);
  free(buffer);
  buffer = NULL;
  return 0;
}

//function used to load the configurations present in the secure config
void read_config(const char* file_name) {
  FILE *conf_file;
  char *line;
  char *equaltok;
  char *temp_equaltok;
  size_t linesize = 1000;
  int size_read = 0;

  if (file_name == NULL) {
    fprintf(ERRORFILE, "Null configuration filename passed in\n");
    exit(INVALID_CONFIG_FILE);
  }

  #ifdef DEBUG
    fprintf(LOGFILE, "read_config :Conf file name is : %s \n", file_name);
  #endif

  //allocate space for ten configuration items.
  config.confdetails = (struct confentry **) malloc(sizeof(struct confentry *)
      * MAX_SIZE);
  if (config.confdetails == NULL) {
      fprintf(ERRORFILE, "malloc failed while reading configuration file.\n");
      exit(OUT_OF_MEMORY);
  }
  config.size = 0;
  conf_file = fopen(file_name, "r");
  if (conf_file == NULL) {
    fprintf(ERRORFILE, "Invalid conf file provided : %s \n", file_name);
    exit(INVALID_CONFIG_FILE);
  }
  while(!feof(conf_file)) {
    line = (char *) malloc(linesize);
    if(line == NULL) {
      fprintf(ERRORFILE, "malloc failed while reading configuration file.\n");
      exit(OUT_OF_MEMORY);
    }
    size_read = getline(&line,&linesize,conf_file);
 
    //feof returns true only after we read past EOF.
    //so a file with no new line, at last can reach this place
    //if size_read returns negative check for eof condition
    if (size_read == -1) {
      free(line);
      line = NULL;
      if(!feof(conf_file)){
        exit(INVALID_CONFIG_FILE);
      } else {
        break;
      }
    }
    int eol = strlen(line) - 1;
    if(line[eol] == '\n') {
        //trim the ending new line
        line[eol] = '\0';
    }
    //comment line
    if(line[0] == '#') {
      free(line);
      line = NULL;
      continue;
    }
    //tokenize first to get key and list of values.
    //if no equals is found ignore this line, can be an empty line also
    equaltok = strtok_r(line, "=", &temp_equaltok);
    if(equaltok == NULL) {
      free(line);
      line = NULL;
      continue;
    }
    config.confdetails[config.size] = (struct confentry *) malloc(
            sizeof(struct confentry));
    if(config.confdetails[config.size] == NULL) {
      fprintf(LOGFILE,
          "Failed allocating memory for single configuration item\n");
      goto cleanup;
    }

    #ifdef DEBUG
      fprintf(LOGFILE, "read_config : Adding conf key : %s \n", equaltok);
    #endif

    memset(config.confdetails[config.size], 0, sizeof(struct confentry));
    const size_t key_tok_len = strlen(equaltok);
    config.confdetails[config.size]->key = (char *) malloc(
            sizeof(char) * (key_tok_len+1));
    if (config.confdetails[config.size]->key == NULL) {
      fprintf(LOGFILE,
          "Failed allocating memory for single configuration item\n");
      goto cleanup;
    }
    memset((void*)config.confdetails[config.size]->key, '\0', key_tok_len+1);
    strncpy((char *)config.confdetails[config.size]->key, equaltok, key_tok_len);
    equaltok = strtok_r(NULL, "=", &temp_equaltok);
    if (equaltok == NULL) {
      fprintf(LOGFILE, "configuration tokenization failed \n");
      goto cleanup;
    }
    //means value is commented so don't store the key
    if(equaltok[0] == '#') {
      free(line);
      line = NULL;
      free((void *)config.confdetails[config.size]->key);
      config.confdetails[config.size]->key = NULL;
      free(config.confdetails[config.size]);
      config.confdetails[config.size] = NULL;
      continue;
    }

    #ifdef DEBUG
      fprintf(LOGFILE, "read_config : Adding conf value : %s \n", equaltok);
    #endif

    const size_t val_tok_len = strlen(equaltok);
    config.confdetails[config.size]->value = (char *) malloc(
            sizeof(char) * (val_tok_len+1));
    if (config.confdetails[config.size]->value == NULL) {
      fprintf(LOGFILE,
          "Failed allocating memory for single configuration item\n");
      goto cleanup;
    }
    memset((void *)config.confdetails[config.size]->value, '\0', val_tok_len+1);
    strncpy((char *)config.confdetails[config.size]->value, equaltok, val_tok_len);
    if((config.size + 1) % MAX_SIZE  == 0) {
      config.confdetails = (struct confentry **) realloc(config.confdetails,
          sizeof(struct confentry **) * (MAX_SIZE + config.size));
      if (config.confdetails == NULL) {
        fprintf(LOGFILE,
            "Failed re-allocating memory for configuration items\n");
        goto cleanup;
      }
    }
    if(config.confdetails[config.size] )
    config.size++;
    free(line);
    line = NULL;
  }
 
  //close the file
  fclose(conf_file);

  if (config.size == 0) {
    fprintf(ERRORFILE, "Invalid configuration provided in %s\n", file_name);
    exit(INVALID_CONFIG_FILE);
  }

  //clean up allocated file name
  return;
  //free spaces alloced.
  cleanup:
  if (line != NULL) {
    free(line);
    line = NULL;
  }
  fclose(conf_file);
  free_configurations();
  return;
}

/*
 * function used to get a configuration value.
 * The function for the first time populates the configuration details into
 * array, next time onwards used the populated array.
 *
 */
char * get_value(const char* key) {
  int count;
  for (count = 0; count < config.size; count++) {
    if (strcmp(config.confdetails[count]->key, key) == 0) {
      return strdup(config.confdetails[count]->value);
    }
  }
  return NULL;
}

/**
 * Function to return an array of values for a key.
 * Value delimiter is assumed to be a comma.
 */
char ** get_values(const char * key) {
  char *value = get_value(key);
  return extract_values(value);
}

/**
 * Extracts array of values from the comma separated list of values.
 */
char ** extract_values(char *value) {
  char ** toPass = NULL;
  char *tempTok = NULL;
  char *tempstr = NULL;
  int size = 0;
  int toPassSize = MAX_SIZE;

  //first allocate any array of 10
  if(value != NULL) {
    toPass = (char **) malloc(sizeof(char *) * toPassSize);
    tempTok = strtok_r((char *)value, ",", &tempstr);
    while (tempTok != NULL) {
      toPass[size++] = tempTok;
      if(size == toPassSize) {
        toPassSize += MAX_SIZE;
        toPass = (char **) realloc(toPass,(sizeof(char *) * toPassSize));
      }
      tempTok = strtok_r(NULL, ",", &tempstr);
    }
  }
  if (size > 0) {
    toPass[size] = NULL;
  }
  return toPass;
}

// free an entry set of values
void free_values(char** values) {
  if (*values != NULL) {
    free(*values);
    *values = NULL;
  }
  if (values != NULL) {
    free(values);
    values = NULL;
  }
}
