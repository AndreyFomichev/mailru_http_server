#!/usr/bin/env tarantool

require'strict'.on()

local http_client = require('http.client')
local tap = require('tap')

local port = os.getenv('PORT')
if port == nil then
    port = 8080
end
print(port)
local URI = string.format("localhost:%d", port)

test_get = tap.test("#0 test GET, get key")
test_get:plan(2)
test_get:test("#1 get value, by key", function(test)
    test:plan(1)
    http_client.post(URI .. "/kv", '{"key":"test", "value":{"first_name" : "Sammy", "last_name" : "Shark", 	"location" : "Ocean", "online" : true,"followers" : 987 }}')
    local resp_positive = http_client.get(URI .. "/kv/test")
    http_client.delete(URI .. "/kv/test")
    test:is(resp_positive.status, 200)
end)


test_get:test("#2 get invalid key", function(test)
    test:plan(1)
    http_client.delete(URI .. "/kv/test")
    local resp_negative = http_client.get(URI .. "/kv/test")
    test:is(resp_negative.status, 404)
end)

test_get:check()

test_post = tap.test("#3 test POST, add key")
test_post:plan(3)
test_post:test("add value by key", function(test)
    test:plan(1)
    local resp_positive = http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    http_client.delete(URI .. "/kv/test")
    test:is(resp_positive.status, 200)
end)
test_post:test("#4 incorrect body", function(test)
    test:plan(3)
    local resp_bad_json = http_client.post(URI .. "/kv", '{"key":"1"')
    test:is(resp_bad_json.status, 400)
    local resp_no_key = 
          http_client.post(URI .. "/kv",'{"value":"1"}')
    test:is(resp_no_key.status, 400)
    local resp_no_value =
        http_client.post(URI .. "/kv",'{"key":"test"}')
		test:is(resp_no_key.status, 400)
end)

test_post:test("#5 existed key", function(test)
    test:plan(1)
    http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    local resp = http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    test:is(resp.status, 409)
    http_client.delete(URI .. "/kv/test")
end)

test_post:check()

test_delete = tap.test("test DELETE, delete key")
test_delete:plan(2)
test_delete:test("delete record", function(test)
    test:plan(1)
    http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    local resp = http_client.delete(URI .. "/kv/test")
    test:is(resp.status, 200)
end)

test_delete:test("#6 get invalid key", function(test)
    test:plan(1)
    http_client.delete(URI .. "/kv/test")
    local resp = http_client.delete(URI .. "/kv/test")
    test:is(resp.status, 404)
end)

test_delete:check()

test_put = tap.test("#7 test PUT, update value")
test_put:plan(3)
test_put:test("update value by key", function(test)
    test:plan(1)
    http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    local resp = http_client.put(URI .. "/kv/test", '{"value":"2"}')
    http_client.delete(URI .. "/kv/test")
    test:is(resp.status, 200)
end)


test_put:test("#8 invalid key", function(test)
    test:plan(1)
    http_client.delete(URI .. "/kv/test")
    local resp = http_client.put(URI .. "/kv/test", '{"value":2}')
    test:is(resp.status, 404)
end)


test_put:test("#9 incorrect body", function(test)
    test:plan(2)
    http_client.post(URI .. "/kv", '{"key":"test", "value":"1"}')
    local resp_bad_json = http_client.put(URI .. "/kv/test", '{"value":"1"')
    test:is(resp_bad_json.status, 400)
    local resp_no_value =
        http_client.put(URI .. "/kv/test",'{"key":"test"}')
    test:is(resp_no_value.status, 400)
    http_client.delete(URI .. "/kv/test")
end)

test_put:check()


os.exit(0)
