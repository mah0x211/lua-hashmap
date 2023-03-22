package = "hashmap"
version = "scm-1"
source = {
    url = "git+https://github.com/mah0x211/lua-hashmap.git",
}
description = {
    summary = "lua-hashmap is a module that can create hashmaps for use between processes built on shared memory.",
    homepage = "https://github.com/mah0x211/lua-hashmap",
    license = "MIT/X11",
    maintainer = "Masatoshi Fukunaga",
}
dependencies = {
    "lua >= 5.1",
    "lauxhlib >= 0.5.0",
}
build = {
    type = "builtin",
    modules = {
        ['hashmap'] = {
            sources = {
                "src/hashmap_lua.c",
                "src/hashmap.c",
            },
        },
    },
}
