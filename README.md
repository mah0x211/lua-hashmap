# lua-hashmap

lua-hashmap module is a module that can create hashmaps for use between processes built on shared memory.


## Installation

```
luarocks install hashmap
```


## Usage

```lua
local hashmap = require('hashmap')
local h = hashmash.new(1000)

print(h:set('hello', 'world!')) -- true
print(h:get('hello')) -- world!

print(h:del('hello')) -- true
print(h:get('hello')) -- nil

print(h:close()) -- true
```


## res, err = hashmap.calc_required_memory_size( [memory_size [, max_buckets [, max_free_blocks [, record_kv_size]]]] )

Calculates the required memory size based on the memory size `memory_size`, maximum number of buckets `max_buckets`, maximum number of free blocks `max_free_blocks`, and the total size of the key and value `record_kv_size` to be stored.


**Parameters**

- `memory_size:integer`: memory size to be used.
- `max_buckets:integer`: maximum number of buckets. (default: `(memory_size / 4) / sizeof(uint64_t)`)
- `max_free_blocks:integer`: maximum number of free blocks. (default: `max_buckets` と同じ値に設定されます)
- `record_kv_size:integer`: total size of the record to be stored.

**Returns**

- `res:table`: table containing the calculation results.
- `err:any`: an error if it fails.


## h, err = hashmap.new( memory_size [, max_buckets [, max_free_blocks]] )

creates a new `hashmap` instance. the size of the memory area actually allocated is aligned.


**Parameters**

- `memory_size:integer`: memory size to be used.
- `max_buckets:integer`: maximum number of buckets. (default: `(memory_size / 4) / sizeof(uint64_t)`)
- `max_free_blocks:integer`: Maximum number of free blocks. (default: set to the same value as `max_buckets`)

**Returns**

- `h:hashmap`: `hashmap` instance.
- `err:any`: an error if it fails.


## ok, err = h:close()

releases the held memory. 

**NOTE:** only the process that created the hashmap instance can release the memory.

**Returns**

- `ok:boolean`: `true` if successful or if already released.
- `err:any`: an error if it fails.


## stat, err = h:stat()

returns statistical information.

**Returns**

- `stat:table`: table containing the statistical information.
- `err:any`: an error if it fails.


## ok, err = h:set( key, val )

stores a `key` and `val`.

**Parameters**

- `key:string`: key string.
- `val:string|boolean|number`: this value is saved in an encoded format as follows:
    - the type information is recorded in the first byte, and the `val` is recorded in the format of the message created from the second byte onwards.  
      the values of type information are as follows:
    - `string`：`0`
    - `boolean`：`1`
    - `number`：`2`
    - `integer`：`4`

**Returns**

- `ok:boolean`: `true` if successful.
- `err:any`: an error if it fails.


## ok, err = h:del( key )

deletes the key and the value associated with the `key`. the deleted memory area is managed as a free block.

**NOTE:** if the number of free blocks reaches `max_free_blocks`, the operation will fail.

**Parameters**

- `key:string`: key string.

**Returns**

- `ok:boolean`: `true` if successful or if the key is not found.
- `err:any`: an error if it fails.


## val, err = h:get( key )

get the value associated with the `key`

**Parameters**

- `key:string`: key string.

**Returns**

- `val:string|boolean|number|integer`: the decoded value of the encoded stored `val`. if not found, returns `nil`.
- `err:any`: an error if it fails.

