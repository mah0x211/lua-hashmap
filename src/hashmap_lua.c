/**
 *  Copyright (C) 2023 Masatoshi Fukunaga
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to
 *  deal in the Software without restriction, including without limitation the
 *  rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
// lua
#include "hashmap.h"
#include "lauxhlib.h"
#include <lauxlib.h>
#include <lua.h>

#define MODULE_MT "hashmap"

typedef struct {
    pid_t pid;
    int closed;
    hashmap_t m;
} lhashmap_t;

enum {
    T_STRING = 0,
    T_BOOLEAN,
    T_NUMBER,
    T_INTEGER,
};

static int decode_value(lua_State *L, const char *val, size_t vlen)
{
    const char t = *val;

    val++;
    vlen--;

    switch (t) {
    case T_STRING:
        lua_pushlstring(L, val, vlen);
        return 1;

    case T_BOOLEAN:
        lua_pushboolean(L, *val);
        return 1;

    case T_NUMBER:
        lua_pushnumber(L, *(lua_Number *)val);
        return 1;

    case T_INTEGER:
        lua_pushinteger(L, *(lua_Integer *)val);
        return 1;

    default:
        lua_pushnil(L);
        lua_pushstring(L, strerror(EBADMSG));
        return 2;
    }
}

static int encode_value(lua_State *L, int idx)
{
    int t         = lua_type(L, idx);
    luaL_Buffer b = {0};

    luaL_buffinit(L, &b);

    switch (t) {
    default:
        return -1;

    case LUA_TSTRING: {
        size_t len    = 0;
        const char *s = lua_tolstring(L, 3, &len);
        luaL_addchar(&b, T_STRING);
        luaL_addlstring(&b, s, len);
    } break;

    case LUA_TBOOLEAN:
        luaL_addchar(&b, T_BOOLEAN);
        luaL_addchar(&b, lua_toboolean(L, 3));
        break;

    case LUA_TNUMBER: {
        if (lauxh_isinteger(L, 3)) {
            lua_Integer n = lua_tointeger(L, 3);
            luaL_addchar(&b, T_INTEGER);
            luaL_addlstring(&b, (char *)&n, sizeof(lua_Integer));
        } else {
            lua_Number n = lua_tonumber(L, 3);
            luaL_addchar(&b, T_NUMBER);
            luaL_addlstring(&b, (char *)&n, sizeof(lua_Number));
        }
    } break;
    }

    luaL_pushresult(&b);
    return 0;
}

static int get_lua(lua_State *L)
{
    lhashmap_t *h   = luaL_checkudata(L, 1, MODULE_MT);
    size_t klen     = 0;
    const char *key = lauxh_checklstring(L, 2, &klen);
    size_t vlen     = 0;
    char *val       = NULL;
    hm_error_t err  = hm_search(&h->m, key, klen, &val, &vlen);

    switch (err) {
    case HM_OK:
        return decode_value(L, val, vlen);

    case HM_NOT_FOUND:
        lua_pushnil(L);
        return 1;

    default:
        lua_pushnil(L);
        lua_pushstring(L, hm_strerror(err));
        return 1;
    }
}

static int del_lua(lua_State *L)
{
    lhashmap_t *h   = luaL_checkudata(L, 1, MODULE_MT);
    size_t klen     = 0;
    const char *key = lauxh_checklstring(L, 2, &klen);
    hm_error_t err  = hm_delete(&h->m, key, klen);

    switch (err) {
    case HM_OK:
    case HM_NOT_FOUND:
        lua_pushboolean(L, 1);
        return 1;

    default:
        lua_pushboolean(L, 0);
        lua_pushstring(L, hm_strerror(err));
        return 2;
    }
}

static int set_lua(lua_State *L)
{
    lhashmap_t *h   = luaL_checkudata(L, 1, MODULE_MT);
    size_t klen     = 0;
    const char *key = lauxh_checklstring(L, 2, &klen);
    int t           = lua_type(L, 3);
    size_t vlen     = 0;
    const char *val = NULL;

    lua_settop(L, 3);
    if (encode_value(L, 3) != 0) {
        lua_pushboolean(L, 0);
        lua_pushstring(L, strerror(EINVAL));
        return 2;
    }
    val = lua_tolstring(L, -1, &vlen);

    hm_error_t err = hm_insert(&h->m, key, klen, val, vlen);
    if (err != HM_OK) {
        lua_pushboolean(L, 0);
        lua_pushstring(L, hm_strerror(err));
        return 2;
    }

    lua_pushboolean(L, 1);
    return 1;
}

static int stat_lua(lua_State *L)
{
    lhashmap_t *h    = luaL_checkudata(L, 1, MODULE_MT);
    hashmap_stat_t s = {0};
    hm_error_t err   = hm_stat(&s, &h->m);

    if (err != HM_OK) {
        lua_pushnil(L);
        lua_pushstring(L, strerror(errno));
        return 2;
    }

    lua_newtable(L);
    lua_pushliteral(L, "metadata");
    lua_newtable(L);
    lauxh_pushint2tbl(L, "memory_size", s.memory_size);
    lauxh_pushint2tbl(L, "max_bucket_flags", s.max_bucket_flags);
    lauxh_pushint2tbl(L, "max_buckets", s.max_buckets);
    lauxh_pushint2tbl(L, "max_free_blocks", s.max_free_blocks);
    lauxh_pushint2tbl(L, "bucket_flags_size", s.bucket_flags_size);
    lauxh_pushint2tbl(L, "buckets_size", s.buckets_size);
    lauxh_pushint2tbl(L, "free_blocks_size", s.free_blocks_size);
    lauxh_pushint2tbl(L, "header_size", s.memory_size);
    lauxh_pushint2tbl(L, "data_size", s.data_size);
    lauxh_pushint2tbl(L, "record_header_size", s.record_header_size);
    lua_settable(L, -3);

    lua_pushliteral(L, "usage");
    lua_newtable(L);
    lauxh_pushint2tbl(L, "used_buckets", s.used_buckets);
    lauxh_pushint2tbl(L, "used_free_blocks", s.used_free_blocks);
    lauxh_pushint2tbl(L, "used_data_size", s.used_data_size);
    lua_settable(L, -3);

    return 1;
}

static int close_lua(lua_State *L)
{
    lhashmap_t *h = luaL_checkudata(L, 1, MODULE_MT);

    if (getpid() != h->pid) {
        lua_pushboolean(L, 0);
        errno = EPERM;
        lua_pushstring(L, strerror(errno));
        return 2;
    } else if (h->closed) {
        lua_pushboolean(L, 1);
        return 1;
    }

    hm_error_t err = hm_destroy(&h->m);
    if (err != HM_OK) {
        lua_pushboolean(L, 0);
        lua_pushstring(L, hm_strerror(err));
        return 2;
    }
    h->closed = 1;
    lua_pushboolean(L, 1);
    return 1;
}

// static int dump_freeblocks_lua(lua_State *L)
// {
//     lhashmap_t *h = luaL_checkudata(L, 1, MODULE_MT);

//     printf("freeblocks: %zu\n", h->m.region->header.num_free_blocks);
//     for (size_t i = 0; i < h->m.region->header.num_free_blocks; i++) {
//         printf("#%zu -> offset %u, size %zu\n", i, h->m.region->freelist[i],
//                *(size_t *)((char *)h->m.region + h->m.region->freelist[i]));
//     }
//     return 0;
// }

static int tostring_lua(lua_State *L)
{
    lua_pushfstring(L, MODULE_MT ": %p", lua_touserdata(L, 1));
    return 1;
}

static int gc_lua(lua_State *L)
{
    lhashmap_t *h = lua_touserdata(L, 1);

    if (!h->closed && getpid() == h->pid) {
        hm_destroy(&h->m);
    }

    return 0;
}

static int new_lua(lua_State *L)
{
    lua_Integer memory_size     = lauxh_checkinteger(L, 1);
    lua_Integer max_buckets     = lauxh_optinteger(L, 2, 0);
    lua_Integer max_free_blocks = lauxh_optinteger(L, 3, 0);
    lhashmap_t *h               = lua_newuserdata(L, sizeof(lhashmap_t));
    hm_error_t err = hm_init(&h->m, memory_size, max_buckets, max_free_blocks);

    switch (err) {
    case HM_OK:
        h->pid    = getpid();
        h->closed = 0;
        lauxh_setmetatable(L, MODULE_MT);
        return 1;

    default:
        lua_pushnil(L);
        lua_pushstring(L, hm_strerror(err));
        return 2;
    }
}

static int calc_required_memory_size_lua(lua_State *L)
{
    hashmap_stat_t s            = {0};
    lua_Integer memory_size     = lauxh_optinteger(L, 1, 0);
    lua_Integer max_buckets     = lauxh_optinteger(L, 2, 0);
    lua_Integer max_free_blocks = lauxh_optinteger(L, 3, 0);
    lua_Integer record_kv_size  = lauxh_optinteger(L, 4, 0);

    if (hm_calc_required_memory_size(&s, memory_size, max_buckets,
                                     max_free_blocks,
                                     record_kv_size) == HM_OK) {
        lua_newtable(L);
        lauxh_pushint2tbl(L, "memory_size", s.memory_size);
        lauxh_pushint2tbl(L, "max_bucket_flags", s.max_bucket_flags);
        lauxh_pushint2tbl(L, "max_buckets", s.max_buckets);
        lauxh_pushint2tbl(L, "max_free_blocks", s.max_free_blocks);
        lauxh_pushint2tbl(L, "bucket_flags_size", s.bucket_flags_size);
        lauxh_pushint2tbl(L, "buckets_size", s.buckets_size);
        lauxh_pushint2tbl(L, "free_blocks_size", s.free_blocks_size);
        lauxh_pushint2tbl(L, "header_size", s.memory_size);
        lauxh_pushint2tbl(L, "data_size", s.data_size);
        lauxh_pushint2tbl(L, "record_header_size", s.record_header_size);
        lauxh_pushint2tbl(L, "record_size", s.record_size);
        return 1;
    }

    lua_pushnil(L);
    lua_pushliteral(L, "cannot calculate required memory size: memory_size "
                       "required if max_buckets is 0");

    return 2;
}

LUALIB_API int luaopen_hashmap(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"close", close_lua},
        {"stat",  stat_lua },
        {"set",   set_lua  },
        {"get",   get_lua  },
        {"del",   del_lua  },
 // {"dump_freeblocks", dump_freeblocks_lua},
        {NULL,    NULL     }
    };

    luaL_newmetatable(L, MODULE_MT);
    // metamethods
    for (struct luaL_Reg *ptr = mmethod; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    for (struct luaL_Reg *ptr = method; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    lua_rawset(L, -3);
    // remove metatable from stack
    lua_pop(L, 1);

    // create module table
    lua_newtable(L);
    lauxh_pushfn2tbl(L, "new", new_lua);
    lauxh_pushfn2tbl(L, "calc_required_memory_size",
                     calc_required_memory_size_lua);

    return 1;
}
