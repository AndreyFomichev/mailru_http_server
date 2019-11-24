#!/usr/bin/env tarantool

json = require('json')
fio = require('fio')
lg = require('log')
--tablex = require "pl.tablex"

local key = 'key'
local value = 'value'

local xlog_dir = 'xlogs'
local snaps_dir = 'snaps'
local log_file = 'logs.log'
local max_rpc = 10

if not fio.path.exists(xlog_dir) then
    fio.mkdir(xlog_dir)
end

if (not fio.path.exists(snaps_dir)) then
    fio.mkdir(snaps_dir)
end

box.cfg {
    log_level = 5;
    wal_dir = xlog_dir;
    memtx_dir = snaps_dir;
    log = log_file;
}

if (not box.space.mailru) then
    sp = box.schema.space.create('mailru')
    sp:create_index('primary', {type = 'hash', parts = {1, 'string'}})
end

local function get_key_value(req)
    local status, body = pcall(req.json, req)
    local key, value = body['key'], body['value']	
    return key, value
end

local function get_value(req)
    local status, body = pcall(req.json, req)
    local value = body['value']	
    return value
end

local function post_method(req)
    lg.info ('post 1' )

	local is_valid_json, k, v = pcall(get_key_value, req)
	if (is_valid_json == false) then
	    lg.info('post, is_valid_json ==false :'..k) 
		return { status = 400 }
    else
	    lg.info('post, is_valid_json ==true : k='..tostring(k))
	end

	lg.info('post, k='..tostring(k))
	
    lg.info ('post, v='..tostring(v))
	
	if (box.stat().rpc ~= nil) then
        if (box.stat().rpc > max_rpc ) then
	        return{ status = 429 }
		end
	end

    if (k == nil or v == nil) then
	    lg.info ('k == nil or v == nil')
        return { status = 400 }
    else
        if (box.space.mailru:get(k) == nil) then
		    box.space.mailru:insert{k, v}
        else
	        lg.info ('box.space.mailru:get(k) != nil')
            return { status = 409 }
        end
    end
	lg.info( 'post finished, k='..tostring(k)..', v='..tostring(v))
end

local function get_method(req)
    if (req == nil) then
	    return { status = 404 }
    end
    local id = req:stash("id")
    --local id = req:param('id')
	lg.info('get, id='..tostring(id))
	if (box.stat().rpc ~= nil) then
        if (box.stat().rpc > max_rpc ) then
	        return{ status = 429 }
		end;
	end
    local obj = box.space.mailru:get(id)
    if (obj == nil) then
        return { status = 404 }
    else
        return req:render({ json = obj })
    end
	
end

local function put_method(req)

    lg.info ('put 1' )
    local id = req:stash("id")
    lg.info('put, id='..tostring(id))
	if (box.stat().rpc ~= nil) then
        if (box.stat().rpc > max_rpc ) then
	        return{ status = 429 }
	    end
	end

	local is_valid_json, v = pcall(get_value, req)
	if (is_valid_json == false) then
	    lg.info('put, is_valid_json ==false :'..v) 
		return { status = 400 }
    else
	    lg.info('put, is_valid_json ==true : v='..tostring(v))
	end

    if (v == nil) then
        return { status = 400 }
    else
        if (box.space.mailru:get(id) == nil) then
            return { status = 404 }
        else
            box.space.mailru:put{id, v}
        end
    end    
end

local function delete_method(req)
    local id = req:stash("id")
	lg.info('delete, id='..tostring(id))
    if (box.stat().rpc ~= nil) then
        if (box.stat().rpc > max_rpc ) then
	        return { status = 429 }
		end
    end
	local obj = box.space.mailru:delete(id)
    if obj == nil then
        return { status = 404 }
    end
	lg.info('delete finished, id='..tostring(id))
    
end

local server = require('http.server').new(nil, 8080)
local router = require('http.router').new(nil, 8080)
server:set_router(router)
router:route({ path = '/kv', method = 'POST'   }, post_method)
router:route({ path = '/', method = 'GET'    }, get_method)
router:route({ path = '/kv/:id', method = 'GET'    }, get_method)
router:route({ path = '/kv/:id', method = 'PUT'    }, put_method)
router:route({ path = '/kv/:id', method = 'DELETE' }, delete_method)
server:start()
