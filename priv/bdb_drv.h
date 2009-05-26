#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>
#include <string.h>
#include <db.h>

// Path to file where data will be stored. 
// It will be created if it doesn't exist
#define DB_PATH "./store.db"

// Binary commands between Erlang VM and Driver
#define CMD_PUT 1
#define CMD_GET 2
#define CMD_DEL 3

// Number of bytes for each key
// (160 bits for SHA1 hash)
#define KEY_SIZE 20

// Define struct to hold state across calls
typedef struct _bdb_drv_t {
  ErlDrvPort port;
 
  DB *db;
} bdb_drv_t;


static ErlDrvData start(ErlDrvPort port, char* cmd);
static void stop(ErlDrvData handle);
static void outputv(ErlDrvData handle, ErlIOVec *ev);
static void put(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void get(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void del(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void unkown(bdb_drv_t *bdb_drv, ErlIOVec *ev);
