package = "indexedRingBuffer"
version = "0.1.0-1"
source = {
  url = "https://github.com/mcvella/indexedRingBuffer.git",
}

description = {
  summary = "Lua implementation of an indexed ring buffer",
  homepage = "https://github.com/mcvella/indexedRingBuffer",
  maintainer = "Matt Vella <matt@mcvella.com>",
  license = "MIT",
}

dependencies = {
  "lua >= 5.1",
  "lua-cjson",
}

build = {
  type = "builtin",
  modules = {
    ["indexedRingBuffer"] = "src/indexedRingBuffer.lua",
    ["cache"] = "src/cache.lua",
    ["dictCache"] = "src/dictCache.lua",
  },
}