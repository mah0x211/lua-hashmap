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
    type = 'make',
    build_variables = {
        LIB_EXTENSION = '$(LIB_EXTENSION)',
        CFLAGS = '$(CFLAGS)',
        WARNINGS = '-Wall -Wno-trigraphs -Wmissing-field-initializers -Wreturn-type -Wmissing-braces -Wparentheses -Wno-switch -Wunused-function -Wunused-label -Wunused-parameter -Wunused-variable -Wunused-value -Wuninitialized -Wunknown-pragmas -Wshadow -Wsign-compare',
        CPPFLAGS = '-I$(LUA_INCDIR)',
        LDFLAGS = '$(LIBFLAG)',
        HASHMAP_COVERAGE = '$(HASHMAP_COVERAGE)',
    },
    install_variables = {
        LIB_EXTENSION = '$(LIB_EXTENSION)',
        LIBDIR = '$(LIBDIR)',
        LUA_INCDIR = '$(LUA_INCDIR)',
    },
}
