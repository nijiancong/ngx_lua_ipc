#include <ngx_lua_ipc.h>
#include <ipc.h>
#include <lauxlib.h>
#include "ngx_http_lua_api.h"

#include <assert.h>

//#define DEBUG_ON

#define LOAD_SCRIPTS_AS_NAMED_CHUNKS

#ifdef DEBUG_ON
#define DBG(fmt, args...) ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "IPC:" fmt, ##args)
#else
#define DBG(fmt, args...) 
#endif
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "IPC:" fmt, ##args)

typedef struct ngx_lua_ipc_main_conf_s  ngx_lua_ipc_main_conf_t;
typedef ngx_int_t (*ngx_lua_ipc_main_conf_handler_pt)(ngx_log_t* log,
                                                      ngx_lua_ipc_main_conf_t* lmcf, lua_State* L);
static u_char* ngx_lua_ipc_rebase_path(ngx_pool_t* pool, u_char* src, size_t len);
static char* ngx_lua_ipc_init_by_lua(ngx_conf_t* cf, ngx_command_t* cmd, void* conf);
static void* ngx_lua_ipc_create_main_conf(ngx_conf_t *cf);
static char* ngx_http_lua_init_main_conf(ngx_conf_t* cf, void* conf);
static int ngx_lua_ipc_traceback(lua_State* L);
static ngx_int_t
ngx_lua_ipc_init_by_file(ngx_log_t* log, ngx_lua_ipc_main_conf_t* lmcf,
                         lua_State* L);

struct ngx_lua_ipc_main_conf_s {
    lua_State*                          L;
    ngx_conf_t*                         cf;
    ngx_lua_ipc_main_conf_handler_pt    init_handler;
    ngx_str_t                           init_src;
};

static ipc_t* ipc = NULL;

static void ngx_lua_ipc_alert_handler(ipc_t* ipc, ngx_pid_t sender_pid, ngx_uint_t sender_slot, ngx_str_t* name, ngx_str_t* data) {
    DBG("ngx_lua_ipc_alert_handler");
    int status = 0;
    u_char* err_msg = NULL;
    size_t len = 0;
    ngx_lua_ipc_main_conf_t* limc = NULL;
    lua_State* L = NULL;
    int base = 0, base1 = 0;

    limc = ngx_http_cycle_get_module_main_conf(ngx_cycle, ngx_lua_ipc_module);
    L = limc->L;

    base = lua_gettop(L);
    lua_getglobal(L, "package");
    lua_getfield(L, -1, "loaded");
    lua_getfield(L, -1, "ngx.ipc");
    lua_getfield(L, -1, "handler");
    if (!lua_isfunction(L , -1)) {
        lua_settop(L, 0);
        return;
    }
    lua_getfield(L, -2, "handlers");
    if (!lua_istable(L, -1)) {
        lua_settop(L, 0);
        return;
    }
    lua_pushlstring(L, (char *) name->data, name->len);
    lua_rawget(L, -2);
    if (lua_type(L, -1) <= 0) {
        lua_settop(L, 0);
        return;
    }
    lua_pushvalue(L, base + 4);
    base1 = lua_gettop(L);
    lua_pushnumber(L, sender_pid);
    lua_pushnumber(L, sender_slot);
    lua_pushlstring(L, (char *) name->data, name->len);
    lua_pushlstring(L, (char *) data->data, data->len);
    lua_pushcfunction(L, ngx_lua_ipc_traceback);
    lua_insert(L, base1);
    status = lua_pcall(L, 4, 0, 1);
    lua_remove(L, base1);
    if (status != 0) {
        err_msg = (u_char *) lua_tolstring(L, -1, &len);
        if (err_msg == NULL) {
            err_msg = (u_char *) "unknown reason";
            len = sizeof("unknown reason") - 1;
        }
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "failed to run ipc.handler: %*s", len, err_msg);
    }
    lua_settop(L, base);    /*  clear remaining elems on stack */

    return;
}

static int push_ipc_return_value(lua_State* L, int success) {
    if(success) {
        lua_pushboolean(L, 1);
        return 1;
    }
    else {
        lua_pushnil(L);
        lua_pushstring (L, ipc_get_last_error(ipc));
        return 2;
    }
}

static ngx_int_t ngx_lua_ipc_get_alert_args(lua_State* L, int stack_offset, ngx_str_t* name, ngx_str_t* data) {
    name->data = (u_char*)luaL_checklstring(L, 1 + stack_offset, &name->len);
    if (lua_gettop(L) >= 2 + stack_offset) {
        data->data = (u_char*)luaL_checklstring(L, 2 + stack_offset, &data->len);
    } else {
        data->len = 0;
        data->data = NULL;
    }

    return NGX_OK;
}

static int ngx_lua_ipc_send_alert_by_pid(lua_State* L) {
    int target_worker_pid = luaL_checknumber(L, 1);
    ngx_int_t rc = 0;
    ngx_str_t name, data;

    ipc->last_error[0] = '\0';
    ngx_lua_ipc_get_alert_args(L, 1, &name, &data);
    rc = ipc_alert_pid(ipc, target_worker_pid, &name, &data);

    return push_ipc_return_value(L, rc == NGX_OK);
}

static int ngx_lua_ipc_send_alert_by_slot(lua_State* L) {
    ngx_uint_t target_slot = luaL_checknumber(L, 1);
    ngx_int_t rc = 0;
    ngx_str_t name, data;

    ipc->last_error[0] = '\0';
    ngx_lua_ipc_get_alert_args(L, 1, &name, &data);
    rc = ipc_alert_slot(ipc, target_slot, &name, &data);

    return push_ipc_return_value(L, rc == NGX_OK);
}

static int ngx_lua_ipc_broadcast_alert(lua_State* L) {
    ngx_str_t name, data;
    ngx_int_t rc = 0;

    ipc->last_error[0] = '\0';
    ngx_lua_ipc_get_alert_args(L, 0, &name, &data);
    rc = ipc_alert_all_workers(ipc, &name, &data);

    return push_ipc_return_value(L, rc == NGX_OK);
}

static int ngx_lua_ipc_get_worker_slot(lua_State* L)
{
    ngx_pid_t pid = luaL_checknumber(L, 1);
    ngx_uint_t slot = ipc_get_slot(ipc, pid);
    if (slot != (ngx_uint_t)NGX_ERROR) {
        lua_pushnumber(L, slot);
        return 1;
    } else {
        lua_pushnil(L);
        lua_pushstring (L, "no slot");
    }
    return 2;
}

static int ngx_lua_ipc_get_worker_pid(lua_State* L)
{
    ngx_uint_t slot = luaL_checknumber(L, 1);
    ngx_pid_t pid = ipc_get_pid(ipc, slot);
    if (pid != NGX_INVALID_PID) {
        lua_pushnumber(L, pid);
        return 1;
    } else {
        lua_pushnil(L);
        lua_pushstring (L, "no pid");
    }
    return 2;
}

static int ngx_lua_ipc_get_worker_pids(lua_State* L) {
    int i = 0, ti = 1, n = 0;
    ngx_pid_t* pids = NULL;

    pids = ipc_get_worker_pids(ipc, &n);

    lua_createtable(L, n, 0);

    for (i = 0; i < n; ++i) {
        lua_pushnumber(L, pids[i]);
        lua_rawseti(L, -2, ti++);
    }

    return 1;
}

static int ngx_lua_ipc_get_other_worker_pids(lua_State* L) {
    int i = 0, ti = 1, n = 0;
    ngx_pid_t* pids = NULL;

    pids = ipc_get_worker_pids(ipc, &n);

    lua_createtable(L, n - 1, 0);

    for (i = 0; i < n; ++i) {
        if(pids[i] != ngx_pid) {
            lua_pushnumber(L, pids[i]);
            lua_rawseti(L, -2, ti++);
        }
    }

    return 1;
}

static int ngx_lua_ipc_init_lua_code(lua_State* L) {
    DBG("ngx_lua_ipc_init_lua_code");
    int status = 0, base = 0;
    const char* msg = NULL;

    ngx_lua_ipc_main_conf_t* lmcf = ngx_http_cycle_get_module_main_conf(ngx_cycle, ngx_lua_ipc_module);

    if (!lmcf->L) {
        lmcf->L = L;
    }
    int t = lua_gettop(L) + 1;
    lua_createtable(L, 0, 7);

    lua_pushcfunction(L, ngx_lua_ipc_send_alert_by_pid);
    lua_setfield(L, t, "send_by_pid");

    lua_pushcfunction(L, ngx_lua_ipc_send_alert_by_slot);
    lua_setfield(L, t, "send_by_slot");

    lua_pushcfunction(L, ngx_lua_ipc_broadcast_alert);
    lua_setfield(L, t, "broadcast");

    lua_pushcfunction(L, ngx_lua_ipc_get_worker_slot);
    lua_setfield(L, t, "get_worker_slot");

    lua_pushcfunction(L, ngx_lua_ipc_get_worker_pid);
    lua_setfield(L, t, "get_worker_pid");

    lua_pushcfunction(L, ngx_lua_ipc_get_worker_pids);
    lua_setfield(L, t, "get_worker_pids");

    lua_pushcfunction(L, ngx_lua_ipc_get_other_worker_pids);
    lua_setfield(L, t, "get_other_worker_pids");

    status = luaL_loadfile(L, (char *) lmcf->init_src.data);
    if (!status) {
        lua_pushvalue(L, t);
        base = t + 1; /* function index */
        lua_pushcfunction(L, ngx_lua_ipc_traceback);  /* push traceback function */
        lua_insert(L, base);  /* put it under chunk and args */
        status = lua_pcall(L, 1, 0, 1);
        lua_remove(L, base);
    }
    if (status && !lua_isnil(L, -1)) {
        msg = lua_tostring(L, -1);
        if (msg == NULL) {
            msg = "unknown error";
        }
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "%s error: %s",
                      "lua_ipc_init_by_file", msg);
    }
    lua_settop(L, t);

    return 1;
}

static ngx_int_t ngx_lua_ipc_init_postconfig(ngx_conf_t* cf) {
    DBG("ngx_lua_ipc_init_postconfig");
    if (ngx_http_lua_add_package_preload(cf, "ngx.ipc", ngx_lua_ipc_init_lua_code) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t ngx_lua_ipc_init_module(ngx_cycle_t* cycle) {
    DBG("ngx_lua_ipc_init_module");
    if (ipc) { //ipc already exists. destroy it!
        ipc_destroy(ipc);
    }
    ipc = ipc_init_module("ngx_lua_ipc", cycle);
    //ipc->track_stats = 1;
    ipc_set_alert_handler(ipc, ngx_lua_ipc_alert_handler);

    return NGX_OK;
}

static ngx_int_t ngx_lua_ipc_init_worker(ngx_cycle_t* cycle) {
    DBG("ngx_lua_ipc_init_worker");
    return ipc_init_worker(ipc, cycle);
}

static void ngx_lua_ipc_exit_worker(ngx_cycle_t* cycle) {
    if (ipc) {
        ipc_destroy(ipc);
        ipc = NULL;
    }
}

static void ngx_lua_ipc_exit_master(ngx_cycle_t* cycle) {
    if (ipc) {
        ipc_destroy(ipc);
        ipc = NULL;
    }
}

u_char* ngx_lua_ipc_rebase_path(ngx_pool_t* pool, u_char* src, size_t len)
{
    u_char *p = NULL;
    ngx_str_t dst;

    dst.data = ngx_palloc(pool, len + 1);
    if (dst.data == NULL) {
        return NULL;
    }

    dst.len = len;

    p = ngx_copy(dst.data, src, len);
    *p = '\0';

    if (ngx_get_full_name(pool, (ngx_str_t *) &ngx_cycle->prefix, &dst)
            != NGX_OK) {
        return NULL;
    }

    return dst.data;
}

static char * ngx_lua_ipc_init_by_lua(ngx_conf_t* cf, ngx_command_t* cmd, void* conf)
{
    DBG("ngx_lua_ipc_init_by_lua");
    u_char* name = NULL;
    ngx_str_t* value = NULL;
    ngx_lua_ipc_main_conf_t* lmcf = conf;

    /*  must specify a content handler */
    if (cmd->post == NULL) {
        return NGX_CONF_ERROR;
    }

    if (lmcf->init_handler) {
        return "is duplicate";
    }

    value = cf->args->elts;

    lmcf->init_handler = (ngx_lua_ipc_main_conf_handler_pt) cmd->post;

    if (cmd->post == ngx_lua_ipc_init_by_file) {
        name = ngx_lua_ipc_rebase_path(cf->pool, value[1].data,
                value[1].len);
        if (name == NULL) {
            return NGX_CONF_ERROR;
        }
        lmcf->init_src.data = name;
        lmcf->init_src.len = ngx_strlen(name);
    }

    return NGX_CONF_OK;
}

static void* ngx_lua_ipc_create_main_conf(ngx_conf_t* cf)
{
    DBG("ngx_lua_ipc_create_main_conf");
    ngx_lua_ipc_main_conf_t* lmcf = NULL;

    lmcf = ngx_pcalloc(cf->pool, sizeof(ngx_lua_ipc_main_conf_t));
    if (lmcf == NULL) {
        return NULL;
    }
    lmcf->L = NULL;
    lmcf->cf = cf;

    return lmcf;
}

static char* ngx_http_lua_init_main_conf(ngx_conf_t* cf, void* conf)
{
    DBG("ngx_lua_ipc_init_main_conf");
    return NGX_CONF_OK;
}

int ngx_lua_ipc_traceback(lua_State* L)
{
    if (!lua_isstring(L, 1)) { /* 'message' not a string? */
        return 1;  /* keep it intact */
    }

    lua_getglobal(L, "debug");
    if (!lua_istable(L, -1)) {
        lua_pop(L, 1);
        return 1;
    }

    lua_getfield(L, -1, "traceback");
    if (!lua_isfunction(L, -1)) {
        lua_pop(L, 2);
        return 1;
    }

    lua_pushvalue(L, 1);  /* pass error message */
    lua_pushinteger(L, 2);  /* skip this function and traceback */
    lua_call(L, 2, 1);  /* call debug.traceback */

    return 1;
}

static ngx_int_t
ngx_lua_ipc_init_by_file(ngx_log_t* log, ngx_lua_ipc_main_conf_t* lmcf,
                         lua_State* L)
{
    return NGX_OK;
}


static ngx_command_t  ngx_lua_ipc_commands[] = {
    { ngx_string("lua_ipc_init_by_lua_file"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_lua_ipc_init_by_lua,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      (void *) ngx_lua_ipc_init_by_file
    },
    ngx_null_command
};

static ngx_http_module_t  ngx_lua_ipc_ctx = {
    NULL,                          /* preconfiguration */
    ngx_lua_ipc_init_postconfig,   /* postconfiguration */
    ngx_lua_ipc_create_main_conf,                          /* create main configuration */
    ngx_http_lua_init_main_conf,                          /* init main configuration */
    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */
    NULL,                          /* create location configuration */
    NULL,                          /* merge location configuration */
};

ngx_module_t  ngx_lua_ipc_module = {
    NGX_MODULE_V1,
    &ngx_lua_ipc_ctx,              /* module context */
    ngx_lua_ipc_commands,          /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    ngx_lua_ipc_init_module,       /* init module */
    ngx_lua_ipc_init_worker,       /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    ngx_lua_ipc_exit_worker,       /* exit process */
    ngx_lua_ipc_exit_master,       /* exit master */
    NGX_MODULE_V1_PADDING
};
