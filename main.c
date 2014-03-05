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

/** 
 * Zookeeper lock recipe in C 
 * modified by Hajo Nils Krabbenhoeft to make it usable as a 
 * command-line tool for distributed exclusive cronjob tasks
 */


#ifdef DLL_EXPORT
#define USE_STATIC_LIB
#endif

#if defined(__CYGWIN__)
#define USE_IPV6
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <zookeeper_log.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <limits.h>
#include <stdbool.h>
#include <zookeeper.h>
#include <pthread.h>
#include <errno.h>

#ifdef HAVE_SYS_UTSNAME_H
#include <sys/utsname.h>
#endif

#ifdef HAVE_GETPWUID_R
#include <pwd.h>
#endif

#define IF_DEBUG(x) if (logLevel==ZOO_LOG_LEVEL_DEBUG) {x;}
#define _LL_CAST_ (long long)




static void free_String_vector(struct String_vector *v) {
    if (v->data) {
        int32_t i;
        for (i=0; i<v->count; i++) {
            free(v->data[i]);
        }
        free(v->data);
        v->data = 0;
    }
}

static int vstrcmp(const void* str1, const void* str2) {
    const char **a = (const char**)str1;
    const char **b = (const char**) str2;
    return strcmp(strrchr(*a, '-')+1, strrchr(*b, '-')+1);
}

static void sort_children(struct String_vector *vector) {
    qsort( vector->data, vector->count, sizeof(char*), &vstrcmp);
}

static char* child_floor(char **sorted_data, int len, char *element) {
    char* ret = NULL;
    int i =0;
    for (i=0; i < len; i++) {
        if (strcmp(sorted_data[i], element) < 0) {
            ret = sorted_data[i];
        }
    }
    return ret;
}

/**
 * get the last name of the path
 */
static char* getName(char* str) {
    char* name = strrchr(str, '/');
    if (name == NULL)
        return NULL;
    return strdup(name + 1);
}

/**
 * just a method to retry get children
 */
static int retry_getchildren(zhandle_t *zh, char* path, struct String_vector *vector, struct timespec *ts, int retry) {
    int ret = ZCONNECTIONLOSS;
    int count = 0;
    while (ret == ZCONNECTIONLOSS && count < retry) {
        ret = zoo_get_children(zh, path, 0, vector);
        if (ret == ZCONNECTIONLOSS) {
            LOG_DEBUG(("connection loss to the server"));
            nanosleep(ts, 0);
            count++;
        }
    }
    return ret;
}

/** see if our node already exists
 * if it does then we dup the name and
 * return it
 */
static char* lookupnode(struct String_vector *vector, char *prefix) {
    char *ret = NULL;
    if (vector->data) {
        int i = 0;
        for (i = 0; i < vector->count; i++) {
            char* child = vector->data[i];
            if (strncmp(prefix, child, strlen(prefix)) == 0) {
                ret = strdup(child);
                break;
            }
        }
    }
    return ret;
}




static const char* state2String(int state){
    if (state == 0)
        return "CLOSED_STATE";
    if (state == ZOO_CONNECTING_STATE)
        return "CONNECTING_STATE";
    if (state == ZOO_ASSOCIATING_STATE)
        return "ASSOCIATING_STATE";
    if (state == ZOO_CONNECTED_STATE)
        return "CONNECTED_STATE";
    if (state == ZOO_EXPIRED_SESSION_STATE)
        return "EXPIRED_SESSION_STATE";
    if (state == ZOO_AUTH_FAILED_STATE)
        return "AUTH_FAILED_STATE";
    
    return "INVALID_STATE";
}

static const char* type2String(int state){
    if (state == ZOO_CREATED_EVENT)
        return "CREATED_EVENT";
    if (state == ZOO_DELETED_EVENT)
        return "DELETED_EVENT";
    if (state == ZOO_CHANGED_EVENT)
        return "CHANGED_EVENT";
    if (state == ZOO_CHILD_EVENT)
        return "CHILD_EVENT";
    if (state == ZOO_SESSION_EVENT)
        return "SESSION_EVENT";
    if (state == ZOO_NOTWATCHING_EVENT)
        return "NOTWATCHING_EVENT";
    
    return "UNKNOWN_EVENT_TYPE";
}

void watcher(zhandle_t *zzh, int type, int state, const char *path, void* context)
{
    fprintf(stderr, "Watcher %s state = %s", type2String(type), state2String(state));
    if (path && strlen(path) > 0) {
      fprintf(stderr, " for path %s", path);
    }
    fprintf(stderr, "\n");

    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            const clientid_t *id = zoo_client_id(zzh);
           	fprintf(stderr, "Got a new session id: 0x%llx\n", _LL_CAST_ id->client_id);
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            fprintf(stderr, "Authentication failure. Shutting down...\n");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            fprintf(stderr, "Session expired. Shutting down...\n");
        }
    }
}



int main( int argc, const char* argv[] )
{
	zhandle_t *zh;
	const char* hosts = argv[1];
	char *path = argv[2];
	struct ACL_vector *acl = &ZOO_OPEN_ACL_UNSAFE;;
	char *id = NULL;
	int isOwner = 0
	char* ownerid = NULL;
	
	// connect
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    zoo_deterministic_conn_order(1); // enable deterministic order
	zh = zookeeper_init(hosts, watcher, 30000, 0, 0, 0);
   	if( !zh ) return errno;
    
    struct Stat stat;
    int exists = zoo_exists(zh, path, 0, &stat);
    int count = 0;
	int maxretry = 5;
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = (.5)*1000000;
    
	// check if folder exists. if not, create
    while ((exists == ZCONNECTIONLOSS || exists == ZNONODE) && (count < maxretry)) {
        count++;
        nanosleep(&ts, 0);

        if (exists == ZCONNECTIONLOSS)
            exists = zoo_exists(zh, path, 0, &stat);
        else if (exists == ZNONODE)
            exists = zoo_create(zh, path, NULL, 0, acl, 0, NULL, 0);
    }
	if( count >= maxretry ) {
        fprintf(stderr, "Could not create %s\n", path);
        goto exitnow;
    }
    
    // lock loop
    count = 0;
    while (count < maxretry) {
        count++;
        nanosleep(&ts, 0);
        
        const clientid_t *cid = zoo_client_id(zh);
        // get the session id
        int64_t session = cid->client_id;
        char prefix[30];
#if defined(__x86_64__)
        snprintf(prefix, 30, "x-%016lx-", session);
#else
        snprintf(prefix, 30, "x-%016llx-", session);
#endif
        struct String_vector vectorst;
        vectorst.data = NULL;
        vectorst.count = 0;
        int ret = retry_getchildren(zh, path, &vectorst, &ts, maxretry);
        if (ret != ZOK) {
            fprintf(stderr, "Could not enumerate folder %s\n", path);
            continue;
        }
        struct String_vector *vector = &vectorst;
        id = lookupnode(vector, prefix);
        free_String_vector(vector);
        if (id == NULL) {
            int len = strlen(path) + strlen(prefix) + 2;
            char buf[len];
            char retbuf[len+20];
            snprintf(buf, len, "%s/%s", path, prefix);
            ret = zoo_create(zh, buf, NULL, 0,  acl, ZOO_EPHEMERAL|ZOO_SEQUENCE, retbuf, (len+20));
            
            // do not want to retry the create since
            // we would end up creating more than one child
            if (ret != ZOK) {
                fprintf(stderr, "Could not create locking node %s\n", buf);
                continue;
            }
            id = getName(retbuf);
        }
        
        if (id != NULL) {
            ret = retry_getchildren(zh, path, vector, &ts, maxretry);
            if (ret != ZOK) {
                fprintf(stderr, "Could not enumerate folder %s\n", path);
                continue;
            }
            //sort this list
            sort_children(vector);
            ownerid = strdup(vector->data[0]);
            char* lessthanme = child_floor(vector->data, vector->count, id);
            if (lessthanme != NULL) {
                int flen = strlen(path) + strlen(lessthanme) + 2;
                char last_child[flen];
                sprintf(last_child, "%s/%s",path, lessthanme);
                printf("LOCKED: %s\n", last_child);
                goto exitnow;
            } else {
                // i got the lock
                break;
            }
            free_String_vector(vector);
            return ZOK;
        }
    }
	if( count >= maxretry ) {
        fprintf(stderr, "Too many retries while trying to lock %s\n", path);
        goto exitnow;
    }
	
	// check that it's locked
	if(id == NULL || ownerid == NULL || (strcmp(id, ownerid) != 0)) {
        goto exitnow;
    }
    
    sleep(10);
    
exitnow:
    zookeeper_close(zh);
    return 0;
}


