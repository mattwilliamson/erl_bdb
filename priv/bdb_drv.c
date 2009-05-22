#include "bdb_drv.h"

static ErlDrvEntry basic_driver_entry = {
    NULL,                             /* init */
    start,                            /* startup */
    stop,                             /* shutdown */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    "bdb_drv",                        /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    NULL,                             /* control */
    NULL,                             /* timeout */
    outputv,                          /* outputv */
    NULL,                             /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL,                             /* event */
    ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT(basic_driver) {
  return &basic_driver_entry;
}

static ErlDrvData start(ErlDrvPort port, char* cmd) {
  bdb_drv_t* retval = (bdb_drv_t*) driver_alloc(sizeof(bdb_drv_t));
  u_int32_t open_flags = DB_CREATE;
  char *path = "test.db";
  DB *db;
  int status;
  
  db_create(&db, NULL, 0);
  status = db->open(db, NULL, path, NULL, DB_BTREE, open_flags, 0);

  retval->port = port;
  retval->db = db;
  
  return (ErlDrvData) retval;
}

static void stop(ErlDrvData handle) {
  bdb_drv_t* driver_data = (bdb_drv_t*) handle;

  driver_data->db->close(driver_data->db, 0);
  driver_free(driver_data);
}

static void outputv(ErlDrvData handle, ErlIOVec *ev) {
  bdb_drv_t* driver_data = (bdb_drv_t*) handle;
  ErlDrvBinary* data = ev->binv[1];
  int command = data->orig_bytes[0];
  
  switch(command) {
  case CMD_PUT:
    put(driver_data, ev);
    break;

  case CMD_GET:
    get(driver_data, ev);
    break;

  default:
    unkown(driver_data, ev);
  }
}

static void put(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* input = ev->binv[1];
  char *bytes = input->orig_bytes;
  char *key = bytes+1;
  char *data = bytes+1+KEY_SIZE;
  int data_size = input->orig_size - 1 - KEY_SIZE;

  db_put(bdb_drv->db, key, data_size, data);

  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),
			   ERL_DRV_ATOM, driver_mk_atom("put"),
			   ERL_DRV_TUPLE, 2};

  driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static void get(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* data = ev->binv[1];
  char *bytes = data->orig_bytes;
  char *key = bytes+1;
  
  ErlDrvBinary* value = db_get(bdb_drv->db, key);
  
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),
			   ERL_DRV_ATOM, driver_mk_atom("get"),
			   //ERL_DRV_BINARY, (ErlDrvTermData) key, KEY_SIZE, 0,
			   ERL_DRV_BINARY, (ErlDrvTermData) value, value->orig_size, 0,
			   ERL_DRV_TUPLE, 3};

  driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  driver_free_binary(value);
}

static void unkown(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* data = ev->binv[1];
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			   ERL_DRV_ATOM, driver_mk_atom("uknown_command"),
			   ERL_DRV_BINARY, (ErlDrvTermData) data, data->orig_size, 0,
			   ERL_DRV_TUPLE, 3};
  driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

int db_put(DB *db, char *key_value, int data_size, char *data_value) {
  int status;
  DBT key;
  DBT data;
  
  bzero(&key, sizeof(DBT));
  bzero(&data, sizeof(DBT));
  
  key.data = key_value;
  key.size = KEY_SIZE;
  
  data.data = data_value;
  data.size = data_size;
  
  status = db->put(db, NULL, &key, &data, 0);
  db->sync(db, 0);
  return status;
}

ErlDrvBinary* db_get(DB *db, char *key_value) {
  ErlDrvBinary *binary;
  int status;
  DBT key;
  DBT data;
  
  bzero(&key, sizeof(DBT));
  bzero(&data, sizeof(DBT));
    
  key.data = key_value;
  key.size = KEY_SIZE;
  
  data.flags = DB_DBT_MALLOC;
  
  status = db->get(db, NULL, &key, &data, 0);

  switch(status) {
  case EINVAL:
    fprintf(stderr, "Bad flag set for get!!!\n");
  case ENOMEM:
    fprintf(stderr, "NOT ENOUGH MEMORY!!!\n");
  case DB_LOCK_DEADLOCK:
    fprintf(stderr, "DEADLOCK!!!\n");
  case DB_SECONDARY_BAD:
    fprintf(stderr, "BAD PRIMARY KEY!!!\n");
  case DB_NOTFOUND:
    fprintf(stderr, "KEY NOT FOUND!!!\n");
  case DB_KEYEMPTY:
    fprintf(stderr, "KEY EMPTY!!!\n");
  }

  binary = driver_alloc_binary(data.size);
  binary->orig_size = data.size;
  //binary->orig_bytes = (char *)&data.data;
  memcpy(binary->orig_bytes, data.data, data.size);

  return binary;
}
