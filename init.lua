---@diagnostic disable: duplicate-set-field
local socket = require("socket")
local timer = require("timer")

table.clear = require("table.clear")

---@class HTTPServer
local http = {
    timeout_length = 5,
    limits_reset_every = 60, -- in seconds
    limit_requests = 512,
    limit_data_tranfer = 1024 * 1024,
    limit_data_send = 1024 * 1024,
    limit_data_receive = 1024 * 1024,
    max_requests = 65536,
    max_data_tranfer = 1024 * 1024 * 16,
    max_data_send = 1024 * 1024 * 16,
    max_data_receive = 1024 * 1024 * 16,
    outgoing_bandwidth = 32768*16, -- in bytes per second per request
    transfer_block_size = 16384,
    sockets = {},
}

local log_file = io.open("connection.log", "ab")
if not log_file then
    log_file = assert(io.open("connection.log", "wb"))
end
log_file:setvbuf("line")

local function log(...)
    log_file:write(...)
    log_file:write("\n")
end

function http.KB(n)  return n * 1000^1 end
function http.KiB(n) return n * 1024^1 end
function http.MB(n)  return n * 1000^2 end
function http.MiB(n) return n * 1024^2 end
function http.GB(n)  return n * 1000^3 end
function http.GiB(n) return n * 1024^3 end
function http.TB(n)  return n * 1000^4 end
function http.TiB(n) return n * 1024^4 end
function http.PB(n)  return n * 1000^5 end
function http.PiB(n) return n * 1024^5 end
function http.EB(n)  return n * 1000^6 end
function http.EiB(n) return n * 1024^6 end
function http.ZB(n)  return n * 1000^7 end
function http.ZiB(n) return n * 1024^7 end
function http.YB(n)  return n * 1000^8 end
function http.YiB(n) return n * 1024^8 end

function http.formatSize(bytes)
    if bytes < 1024 then
        return string.format("%.0fB", bytes)
    elseif bytes < http.MiB(1) then
        return string.format("%.3fKiB", bytes / http.KiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.GiB(1) then
        return string.format("%.3fMiB", bytes / http.MiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.TiB(1) then
        return string.format("%.3fGiB", bytes / http.GiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.PiB(1) then
        return string.format("%.3fTiB", bytes / http.TiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.EiB(1) then
        return string.format("%.3fPiB", bytes / http.PiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.ZiB(1) then
        return string.format("%.3fEiB", bytes / http.EiB(1)):gsub("%.?0+(...)$", "%1")
    elseif bytes < http.YiB(1) then
        return string.format("%.3fZiB", bytes / http.ZiB(1)):gsub("%.?0+(...)$", "%1")
    else
        return string.format("%.3fYiB", bytes / http.YiB(1)):gsub("%.?0+(...)$", "%1")
    end
end

local errors = {
    max_transfer_exceeded = -1000,
}

http.tcp = nil

local threads = {}
local clients = {}
local buffers = {}
local limits = {
    transfer = {},
    requests = {},
}

local function getClientData(client)
    if not clients[client] then
        clients[client] = {
            id = client:getpeername(),
            last_ping = 0,
            total_input = 0,
            total_output = 0,
            total_bandwidth = 0,
            total_requests = 0,
        }
        local client = clients[client]
        if not limits.transfer[client.id] then
            limits.transfer[client.id] = {input = 0, output = 0, bandwidth = 0, total_input = 0, total_output = 0, total_bandwidth = 0}
            table.insert(limits.transfer, limits.transfer[client.id])
        end
        if not limits.requests[client.id] then
            limits.requests[client.id] = {count = 0, total_count = 0}
            table.insert(limits.requests, limits.requests[client.id])
        end
        client.transfer = limits.transfer[client.id]
        client.requests = limits.requests[client.id]
    end
    return clients[client]
end

function http:getClientData(client)
    return getClientData(client)
end

local function closeClient(client, safe)
    local data = getClientData(client)
    local name = client:getpeername() or "Unknown"
    log(name, ":", string.rep(" ", 72 - #name), " the connection has been closed. ", http.formatSize(data.transfer.total_input), " REC ", http.formatSize(data.transfer.total_output), " SEND")
    clients[client] = nil
    client:shutdown("send")
    client:shutdown()
    client:close()
    if not safe then
        error("client has been closed")
    end
end

local function abortIfInvalid(client)
    if http:shouldAbortClientConnection(client) then
        closeClient(client, true)
        error("the connection was aborted")
    end
end

local last_step = timer.time
local last_limit_refresh = 0
local function getQuotaString(client)
    local data = getClientData(client)
    return string.format("\nRequests: %d / %d in last minute; %d / %d in past day\
    Data transferred: %s / %s in last minute; %s / %s in past day\
    Data uploaded: %s / %s in last minute; %s / %s in past day\
    Data downloaded: %s / %s in last minute; %s / %s in past day\
    Next minutely reset is in %ds",
    data.requests.count or -1, http.limit_requests, data.requests.total_count or -1, http.max_requests,
    http.formatSize(data.transfer.bandwidth or -1), http.formatSize(http.limit_data_tranfer), http.formatSize(data.transfer.total_bandwidth or -1), http.formatSize(http.max_data_tranfer),
    http.formatSize(data.transfer.input or -1), http.formatSize(http.limit_data_receive), http.formatSize(data.transfer.total_input or -1), http.formatSize(http.max_data_receive),
    http.formatSize(data.transfer.output or -1), http.formatSize(http.limit_data_send), http.formatSize(data.transfer.total_output or -1), http.formatSize(http.max_data_send), http.limits_reset_every - (os.time() - last_limit_refresh))
end

local function countBandwidth(client, input, output)
    local client_data = getClientData(client)
    client_data.transfer.total_input = client_data.transfer.total_input + input
    client_data.transfer.total_output = client_data.transfer.total_output + output
    client_data.transfer.total_bandwidth = client_data.transfer.total_bandwidth + output + input
    client_data.transfer.input = client_data.transfer.input + input
    client_data.transfer.output = client_data.transfer.output + output
    client_data.transfer.bandwidth = client_data.transfer.bandwidth + output + input
end

local function yield(client)
    if timer.time - getClientData(client).last_ping > http.timeout_length then
        return true
    end
    coroutine.yield(client)
    return false
end

local function pinged(client)
    getClientData(client).last_ping = timer.time
end

local function readClientBlock(client)
    local data, err, partial = client:receive(1024)
    if not data then data = partial end
    if data then
        buffers[client] = buffers[client]..data
        countBandwidth(client, #data, 0)
        pinged(client)
        return data ~= ""
    end
end

local function readClient(client, mode)
    if not buffers[client] then buffers[client] = "" end
    if type(mode) == "number" then
        while #buffers[client] < mode do
            if not readClientBlock(client) then yield(client) end
            if http:shouldAbortClientConnection(client) then return nil, errors.max_transfer_exceeded end
        end
        local b = buffers[client]
        buffers[client] = b:sub(mode + 1, -1)
        return b:sub(1, mode)
    elseif mode == "*l" then
        while not buffers[client]:find("\n") do
            if not readClientBlock(client) then yield(client) end
            if http:shouldAbortClientConnection(client) then return nil, errors.max_transfer_exceeded end
        end
        local b = buffers[client]
        local at = b:find("\n")
        buffers[client] = b:sub(at + 1, -1)
        if b:sub(at - 1, at - 1) == "\r" then at = at - 1 end
        return b:sub(1, at - 1)
    end
end

local function getHeaders(client)
    local method, resource
    local headers = {}
    while true do
        local line = readClient(client, "*l")
        if not line then
            if yield(client) then return end
            goto continue
        end
        pinged(client)
        if line == "" then break end
        if not method then
            method, resource = line:match("^(%u+) ([^ ]*)")
        else
            table.insert(headers, line)
        end
        if not method then
            return
        end
        ::continue::
    end
    return method, resource, headers
end

local function performThrottledSend(client, data)
    local t = timer.time
    local sent = 1
    while sent <= #data do
        if sent >= (timer.time - t) * http.outgoing_bandwidth then
            coroutine.yield(true, client)
        else
            local substr = data:sub(sent, sent + http.transfer_block_size - 1)
            client:send(substr)
            sent = sent + http.transfer_block_size
            pinged(client)
            countBandwidth(client, 0, #substr)
        end
    end
end

local function clientThread(client)
    if http:countAdditionalQueries(client, 1) then
        return false, client
    end
    local method, resource, headers = getHeaders(client)
    if not method then
        if http:shouldAbortClientConnection(client) then
            return false, client
        end
        closeClient(client)
        return false
    end
    if resource == "/http-ip-limits" then
        local page = getQuotaString(client)
        performThrottledSend(client, page)
        closeClient(client)
        return false
    end
    local page, param = http:resolveResource(resource, client)
    if type(page) == "string" then
        page = page.."\n\n"..getQuotaString(client)
        performThrottledSend(client, page)
        closeClient(client)
        return false
    end
    if page then
        local status, headers, body = page(client, method, headers, param)
        table.insert(headers, "Content-Length: "..#body)
        local str = {"HTTP/1.1 "..(status or 501).." Gooba", table.concat(headers, "\r\n"), "", body}
        local page = table.concat(str, "\r\n")
        performThrottledSend(client, page)
        closeClient(client)
        return false
    else
        local page = "HTTP/1.1 404"
        client:send(page)
        countBandwidth(client, 0, #page)
        closeClient(client)
        return false
    end
    return false
end

function http:readFromClient(client, mode)
    local result = readClient(client, mode)
    abortIfInvalid(client)
    return result
end

function http:shouldAbortClientConnection(client)
    local data = getClientData(client)
    if timer.time - data.last_ping > http.timeout_length then
        return true
    elseif data.requests.count > self.limit_requests then
        return true
    elseif data.transfer.bandwidth > self.limit_data_tranfer then
        return true
    elseif data.transfer.output > self.limit_data_send then
        return true
    elseif data.transfer.input > self.limit_data_receive then
        return true
    elseif data.requests.total_count > self.max_requests then
        return true
    elseif data.transfer.total_bandwidth > self.max_data_tranfer then
        return true
    elseif data.transfer.total_output > self.max_data_send then
        return true
    elseif data.transfer.total_input > self.max_data_receive then
        return true
    else
        return false
    end
end

function http:shouldRejectClientConnection(client)
    local data = getClientData(client)
    if (data.requests.count - math.min(60, self.limit_requests / 2)) > self.limit_requests then
        return true
    elseif data.transfer.bandwidth / 1.1 > self.limit_data_tranfer then
        return true
    elseif data.transfer.output / 1.1 > self.limit_data_send then
        return true
    elseif data.transfer.input / 1.1 > self.limit_data_receive then
        return true
    elseif data.requests.total_count / 1.1 > self.max_requests then
        return true
    elseif data.transfer.total_bandwidth / 1.1 > self.max_data_tranfer then
        return true
    elseif data.transfer.total_output / 1.1 > self.max_data_send then
        return true
    elseif data.transfer.total_input / 1.1 > self.max_data_receive then
        return true
    else
        return false
    end
end

function http:getUsagePercentage(client)
    local data = getClientData(client)
    return 100 * math.max(
        data.requests.count / self.limit_requests,
        data.transfer.bandwidth / self.limit_data_tranfer,
        data.transfer.output / self.limit_data_send,
        data.transfer.input / self.limit_data_receive,
        data.requests.total_count / self.max_requests,
        data.transfer.total_bandwidth / self.max_data_tranfer,
        data.transfer.total_output / self.max_data_send,
        data.transfer.total_input / self.max_data_receive
    )
end

function http:countAdditionalQueries(client, qty)
    local data = getClientData(client)
    data.requests.count = data.requests.count + qty
    data.requests.total_count = data.requests.total_count + qty
    return self:shouldAbortClientConnection(client)
end

function http:host(ip, port)
    local tcp = socket.tcp()
    local s, e = tcp:bind(ip, port)
    tcp:settimeout(0, "t")
    tcp:listen(500)
    table.insert(self.sockets, tcp)
    return s, e
end

function http:getClient()
    assert(#self.sockets > 0, "Server is not bound to any interface!")
    for k, tcp_socket in ipairs(self.sockets) do
        local client = tcp_socket:accept()
        if client then
            local name = client:getpeername() or "Unknown"
            if self:shouldRejectClientConnection(client) then
                log(name, ":", string.rep(" ", 72 - #name), " initiated a connection attempt that was rejected")
                client:close()
            else
                log(name, ":", string.rep(" ", 72 - #name), " has initiated a connection")
                client:settimeout(0)
                return client
            end
        end
    end
end

local function handleRoutine(thread, index, ...)
    local success, continue, client = coroutine.resume(thread, ...)
    local abort = (type(client) == "userdata" and http:shouldAbortClientConnection(client))
    if not success and not continue:find("client has been closed") then
        print(continue)
    end
    if not success or not continue or abort then
        table.remove(threads, index)
        if client and type(client) == "userdata" then
            local str = "HTTP/1.1 429\r\nContent-Type: text/plain\r\n\r\n"..getQuotaString(client)
            client:send(str)
            countBandwidth(client, 0, #str)
            closeClient(client, true)
        end
    end
end

function http:accept()
    local client = self:getClient()
    if client then
        local thread = coroutine.create(function(...)
            local tb
            xpcall(clientThread, function(err)
                tb = debug.traceback(err)
            end, ...)
            if tb then
                error(tb)
            end
        end)
        table.insert(threads, thread)
        pinged(client)
        handleRoutine(thread, #threads, client)
    end
end

function http:step()
    if os.time() >= last_limit_refresh + self.limits_reset_every then
        local do_daily = last_limit_refresh % 86400 > 43200 and os.time() % 86400 < 43200
        last_limit_refresh = os.time()
        for k, v in ipairs(limits.transfer) do
            v.bandwidth = math.max(0, v.bandwidth - self.limit_data_tranfer)
            v.input = math.max(0, v.input - self.limit_data_receive)
            v.output = math.max(0, v.output - self.limit_data_send)
        end
        for k, v in ipairs(limits.requests) do
            v.count = math.max(0, v.count - self.limit_requests)
        end
        if do_daily then
            for k, v in ipairs(limits.transfer) do
                v.total_bandwidth = math.max(0, v.total_bandwidth - self.max_data_tranfer)
                v.total_input = math.max(0, v.total_input - self.max_data_receive)
                v.total_output = math.max(0, v.total_output - self.max_data_send)
            end
            for k, v in ipairs(limits.requests) do
                v.total_count = math.max(0, v.total_count - self.max_requests)
            end
        end
    end
    local t = timer.time
    local dt = t - last_step
    for k = #threads, 1, -1 do
        local thread = threads[k]
        handleRoutine(thread, k)
    end
    http:accept()
    last_step = t
end

function http:resolveResource(resource, client)
    if resource == "/" or resource == "/index" then
        return "This is an example page. Replace http:resolveResource() with your own page resolver to display your own webpages."
    end
    return nil
end

return http
