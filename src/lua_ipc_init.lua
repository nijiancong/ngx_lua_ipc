local ipc = ...
local ngx = ngx
local tablepool = require "tablepool"
local pool_name = "ngx_lua_ipc"

local _M = { _VERSION = '0.1' }
local mt = { __index = _M }

function _M.new(self)
    return setmetatable(
    {
        list = {first = 0, last = -1}
    },mt)
end

function _M.size(self)
    local list = self.list
    return list.last - list.first + 1
end

function _M.lpush(self, value)
    local list = self.list
    local first = list.first - 1
    list.first = first
    list[first] = value
end

function _M.rpush(self, value)
    local list = self.list
    local last = list.last + 1
    list.last = last
    list[last] = value
end

function _M.lpop(self)
    local list = self.list
    local first = list.first
    if first > list.last then
        return nil, "empty!"
    end
    local value = list[first]
    list[first] = nil
    list.first = first + 1
    return value
end

function _M.rpop(self)
    local list = self.list
    local last = list.last
    if list.first > last then
        return nil, "empty!"
    end
    local value = list[last]
    list[last] = nil
    list.last = last - 1
    return value
end

function _M.top(self)
    local list = self.list
    return list[list.first]
end

function _M.tail(self)
    local list = self.list
    return list[list.last]
end

local Queue = _M
local handler_queue = Queue:new()
local handlering = false
local handlers = {}
local sender = {}
local cache_slot2pid = {}
local cache_pid2slot = {}

local function to_handler()
    handlering = true
    local i = 0
    local v = handler_queue:lpop()
    while v do
        sender.pid = v[1]
        sender.slot = v[2]
        ipc.sender = sender
        handlers[v[3] ](v[4])
        ipc.sender = nil
        i = i + 1
        if i == 200 then
            ngx.sleep(0)
            i = 0
        end
        tablepool.release(pool_name, v, true)
        v = handler_queue:lpop()
    end
    handlering = false
end

local handler = function(pid, slot, name, data)
    local v = tablepool.fetch(pool_name, 4, 0)
    v[1] = pid
    v[2] = slot
    v[3] = name
    v[4] = data
    handler_queue:rpush(v)
    if not handlering then
        to_handler()
    end
end

local receive = function(name, handler)
    local type_name = type(name)
    local type_handler = type(handler)
    if type_name == "string" and type_handler == "function" then
        handlers[name] = handler
    elseif type_name == "table" then
        for n, h in pairs(name) do
            if type(n) == "string" and type(h) == "function" then
                handlers[n] = h
            end
        end
    end
end

local reply = function(name, data)
    if not ipc.sender then
        error("Can't reply, ngx.ipc.reply called outside of IPC alert handler.")
    end
    return ipc.send_by_slot(ipc.sender.slot, name, data)
end

local send = function(pid, name, data)
    local slot = cache_pid2slot[pid]
    if not slot then
        return ipc.send_by_pid(pid, name, data)
    end
    return ipc.send_by_slot(slot, name, data)
end

local get_cache_pids = function()
    return cache_slot2pid, cache_pid2slot
end

receive("_lua_ipc_init_", function(data)
    local slot = ipc.sender.slot
    local new_pid = ipc.sender.pid
    local old_pid = cache_slot2pid[slot]
    if old_pid and old_pid ~= new_pid then
        cache_pid2slot[old_pid] = nil
    end
    cache_slot2pid[slot] = new_pid
    cache_pid2slot[new_pid] = slot
    reply("_lua_ipc_init_reply_", "Hello!")
end)

receive("_lua_ipc_init_reply_", function(data)
    local slot = ipc.sender.slot
    local new_pid = ipc.sender.pid
    local old_pid = cache_slot2pid[slot]
    if old_pid and old_pid ~= new_pid then
        cache_pid2slot[old_pid] = nil
    end
    cache_slot2pid[slot] = new_pid
    cache_pid2slot[new_pid] = slot
end)

ngx.timer.at(0.001, function(premature)
    ipc.broadcast("_lua_ipc_init_", "Hello every one!")
end)

ipc.handler = handler
ipc.handlers = handlers
ipc.receive = receive
ipc.reply = reply
ipc.send = send
ipc.get_cache_pids = get_cache_pids
