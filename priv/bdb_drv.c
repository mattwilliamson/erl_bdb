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
  u_int32_t open_flags = DB_CREATE | DB_THREAD;
  DB *db;
  int status;
  
  db_create(&db, NULL, 0);
  status = db->open(db, NULL, DB_PATH, NULL, DB_BTREE, open_flags, 0);

  if(status != 0) {
    char *error_reason;

    switch(status){
    case DB_OLD_VERSION:
      error_reason = "the file was created with a different version.";
      break;

    case EINVAL:
      error_reason = "the file was opened with incorrect flags. Perhaps the system does not support the DB_THREAD flag?";
      break;

    case DB_RUNRECOVERY:
      error_reason = "the file needs to be recovered, it may be corrupt.";
      break;
    default:
      error_reason = "of an unkown reason.";
    }

    fprintf(stderr, "Unabled to open file: %s because %s\n\n", DB_PATH, error_reason);
  }

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

  case CMD_DEL:
    del(driver_data, ev);
    break;

  default:
    unkown(driver_data, ev);
  }
}

static void put(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* input = ev->binv[1];
  char *bytes = input->orig_bytes;
  char *key_bytes = bytes+1;
  char *value_bytes = bytes+1+KEY_SIZE;
  int value_size = input->orig_size - 1 - KEY_SIZE;
  
  DB *db = bdb_drv->db;
  DBT key;
  DBT value;
  int status;

  bzero(&key, sizeof(DBT));
  bzero(&value, sizeof(DBT));
  
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  value.data = value_bytes;
  value.size = value_size;
  
  status = db->put(db, NULL, &key, &value, 0);
  db->sync(db, 0);

  if(status == 0) {
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};

    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  } else {
    char * error_reason;

    switch(status) {
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case EACCES:
      error_reason = "readonly";
      break;
    case EINVAL:
      error_reason = "badflag";
      break;
    case ENOSPC:
      error_reason = "btree_max";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unkown";
    }
    
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};

    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}

static void get(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* input = ev->binv[1];
  ErlDrvBinary *output_bytes;
  char *bytes = input->orig_bytes;
  char *key_bytes = bytes+1;
  
  DB *db = bdb_drv->db;
  DBT key;
  DBT value;
  int status;
  
  bzero(&key, sizeof(DBT));
  bzero(&value, sizeof(DBT));
    
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  value.flags = DB_DBT_MALLOC; // Don't forget to free
  
  status = db->get(db, NULL, &key, &value, 0);

  if(status == 0) {
    output_bytes = driver_alloc_binary(value.size);
    output_bytes->orig_size = value.size;
    // TODO:Figure out if we can somehow use this original memory
    //binary->orig_bytes = (char *)&data.data;
    memcpy(output_bytes->orig_bytes, value.data, value.size);
    free(value.data);
    
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),
			     ERL_DRV_BINARY, (ErlDrvTermData) output_bytes, output_bytes->orig_size, 0,
			     ERL_DRV_TUPLE, 2};
    
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
    driver_free_binary(output_bytes);
  } else {
    char *error_reason;

    switch(status) {
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case DB_SECONDARY_BAD:
      error_reason = "bad_secondary_index";
      break;
    case ENOMEM:
      error_reason = "insufficient_memory";
      break;
    case EINVAL:
      error_reason = "bad_flag";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unknown";
    }
    
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}

static void del(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* data = ev->binv[1];
  char *bytes = data->orig_bytes;
  char *key_bytes = bytes+1;
  
  DB *db = bdb_drv->db;
  DBT key;
  int status;
  
  bzero(&key, sizeof(DBT));
    
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  status = db->del(db, NULL, &key, 0);
  db->sync(db, 0);
  
  if(status == 0) {
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};
    
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
    
  } else {
    char *error_reason;

    switch(status) {
    case DB_NOTFOUND:
      error_reason = "not_found";
      break;
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case DB_SECONDARY_BAD:
      error_reason = "bad_secondary_index";
      break;
    case EINVAL:
      error_reason = "bad_flag";
      break;
    case EACCES:
      error_reason = "readonly";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unknown";
    }
    
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};

    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}

static void unkown(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			   ERL_DRV_ATOM, driver_mk_atom("uknown_command"),
			   ERL_DRV_TUPLE, 2};
  driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}
