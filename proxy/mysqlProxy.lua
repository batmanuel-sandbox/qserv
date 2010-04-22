
-- For questions, contact Jacek Becla


require ("xmlrpc.http")

-- todos:
--  * support "objectId IN", DESCRIBE
--
--  * communication with user, returning results etc
--
--  * improve error checking:
--    - enforce single bounding box
--    - check if there is "objectId=" or "objectId IN" in the section
--      of query which we are skipping
--    - block "SHOW" except "SHOW DATABASES" and "SHOW TABLES"
--
--  * supress errors "FUNCTION proxyTest.areaSpec_box does not exist"
--
--  * test what happens when I change db (use x; later use y)




-- API between lua and qserv:
--  * invoke(cleanQueryString, hintString)
--  * cleanQueryString is the query without special hints: objectId 
--    related hints are passed through, but bounding boxes are not
--  * hintString: "box", "1,2,11,12", "box", "5,55,6,66", 
--    "objectId", "3", "objectId", "5,6,7,8" and so on


-------------------------------------------------------------------------------
--                        global variables (yuck)                            --
-------------------------------------------------------------------------------

rpcHost = "127.0.0.1"
rpcPort = 7080
rpcHP = "http://" .. rpcHost .. ":" .. rpcPort


-- constants (kind of)
ERR_AND_EXPECTED   = -4001
ERR_BAD_ARG        = -4002
ERR_NOT_SUPPORTED  = -4003
ERR_OR_NOT_ALLOWED = -4004
ERR_RPC_CALL       = -4005


-- query string and array containing hints
-- These two will be passed to qserv
queryToPassStr = ""
hintsToPassArr = {}


-------------------------------------------------------------------------------
--                             error handling                                --
-------------------------------------------------------------------------------

function errors ()
    local self = { __errNo__ = 0, __errMsg__ = "" }

    local set = function(errNo, errMsg)
        __errNo__  = errNo
        __errMsg__ = errMsg
        return errNo
    end

    local append = function(errMsg)
        __errMsg__ = __errMsg__ .. errMsg
        return errNo
    end

    ---------------------------------------------------------------------------

    local send = function()
        local e = -1 * __errNo__ -- mysql doesn't like negative errors
        proxy.response = {
            type     = proxy.MYSQLD_PACKET_ERR,
            errmsg   = __errMsg__,
            errcode  = e,
            sqlstate = 'Proxy',
        }
        print ("ERROR "..e..": "..__errMsg__)
        return proxy.PROXY_SEND_RESULT
    end

    local setAndSend = function(errNo, errMsg)
        set(errNo, errMsg)
        return send()
    end

    ---------------------------------------------------------------------------

    return {
        set = set,
        append = append,
        send = send,
        setAndSend = setAndSend
    }
end


err = errors()


-------------------------------------------------------------------------------
--                       random util functions                               --
-------------------------------------------------------------------------------

function utilities()

    local tableToString = function(t)
        local s = ""
        for k,v in pairs(t) do 
            s = s .. '"' .. k .. '" "' .. v .. '" '
        end
        return s
    end

    ---------------------------------------------------------------------------

    -- tokenizes string with comma separated values,
    -- returns a table
    local csvToTable = function(s)
        s = s .. ','        -- ending comma
        local t = {}        -- table to collect fields
        local fieldstart = 1
        repeat
            -- next field is quoted? (start with `"'?)
            if string.find(s, '^"', fieldstart) then
                local a, c
                local i  = fieldstart
                repeat
                    -- find closing quote
                    a, i, c = string.find(s, '"("?)', i+1)
                until c ~= '"'    -- quote not followed by quote?
                if not i then error('unmatched "') end
                local f = string.sub(s, fieldstart+1, i-1)
                table.insert(t, (string.gsub(f, '""', '"')))
                fieldstart = string.find(s, ',', i) + 1
            else                -- unquoted; find next comma
                local nexti = string.find(s, ',', fieldstart)
                table.insert(t, string.sub(s, fieldstart, nexti-1))
                fieldstart = nexti + 1
            end
        until fieldstart > string.len(s)
        return t
    end

    ---------------------------------------------------------------------------

    local removeExtraWhiteSpaces = function (q)
        -- convert new lines and tabs to a space
        q = string.gsub(q, '[\n\t]+', ' ')

        -- remove all spaces before/after '='
        q = string.gsub(q, '[ ]+=', '=')
        q = string.gsub(q, '=[ ]+', '=')

        -- remove all spaces before/after ','
        q = string.gsub(q, '[ ]+,', ',')
        q = string.gsub(q, ',[ ]+', ',')

        -- remove all spaces before/after '(' and before ')'
        q = string.gsub(q, "[ ]+%(", '%(')
        q = string.gsub(q, "%([ ]+", '%(')
        q = string.gsub(q, '[ ]+%)', '%)')

        -- convert multiple spaces to a single space
        q = string.gsub(q, '[ ]+', ' ')

        return q
    end

    ---------------------------------------------------------------------------
    return {
        tableToString = tableToString,
        csvToTable = csvToTable,
        removeExtraWhiteSpaces = removeExtraWhiteSpaces
    }
end

utils = utilities()


-------------------------------------------------------------------------------
--                                 parser                                    --
-------------------------------------------------------------------------------

function miniParser()
    local self = { haveWhere = false, andNeeded = false }

    local setAndNeeded = function()
        andNeeded = true
    end

    local addWhereAndIfNeeded = function()
        if not haveWhere then
            queryToPassStr = queryToPassStr .. 'WHERE'
            haveWhere = true
        elseif andNeeded then
            queryToPassStr = queryToPassStr .. ' AND'
        end
    end

    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    local parseAreaSpecBox = function(s)
        local p1 = string.len("QSERV_AREASPEC_BOX")
        -- print ("parsing args for areaspecbox: '" .. string.sub(s, p1) .. "'")
        local p2 = string.find(s, ')')
        if p2 then
            -- skip " (" in front and ")" in the end
            local params = string.sub(s, p1+2, p2-1)
            hintsToPassArr["box"] = string.sub(params, 0)

            t = utils.csvToTable(params)
            if not 4 == table.getn(t) then
                return err.set(ERR_BAD_ARG, "Incorrect number of arguments " ..
                                 "after qserv_areaSpec_box: '"..params.."'")
            end
            -- addWhereAndIfNeeded()
            -- queryToPassStr = queryToPassStr .. 
            --               " ra BETWEEN "..t[1].." AND "..t[3].." AND"..
            --               " decl BETWEEN "..t[2].." AND "..t[4]
            -- parser.setAndNeeded()
            return p2
        end
        return err.set(ERR_BAD_ARG, 
              "Invalid arguments after qserv_areaSpec_box: '"..params.."'")
    end

    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    local parseObjectId = function(s)
        local p1 = string.len("OBJECTID=")
        -- print ("parsing args for objectId: '" .. string.sub(s, p1) .. "'")
        local p2 = string.find(s, ' ')
        if p2 then
            local params = string.sub(s, p1+1, p2)
            params = string.gsub(params, ' ', '')
            hintsToPassArr["objectId"] = params
            addWhereAndIfNeeded()
            queryToPassStr = queryToPassStr..' objectId='..params
            parser.setAndNeeded()
            return p2-1
        end
        return err.set(ERR_BAD_ARG, "Invalid argument")
    end

    ---------------------------------------------------------------------------

    -- Output:
    --    negative: failure
    --    positive: number of characters processed by this function
    -- This function currently detects the following special tokens:
    --   AREASPEC_BOX
    --   OBJECTID
    --   OBJECTID IN
    local parseIt = function(q, p)
        -- print ("args:"..p..", "..string.sub(q, 0, p))

        -- String that has not been parsed yet
        local s = string.sub(q, p)

        -- Number of characters parsed already, counting 
        -- from the beginning of q
        local nParsed = p

        -- Value that will be returned from this function
        local retV = 0

        while true do
            local tokenFound = false
            if string.find(s, "^QSERV_AREASPEC_BOX") then
                local c = parseAreaSpecBox(s)
                if c < 0 then
                    return c
                end
                nParsed = nParsed + c
                s = string.sub(q, nParsed)
                tokenFound = true
            elseif string.find(s, "^OBJECTID=") then
                local c = parseObjectId(s)
                if c < 0 then
                    return err.append(" ("..s..")")
                end
                nParsed = nParsed + c
                s = string.sub(q, nParsed)
                tokenFound = true
            elseif string.find(s, "^OBJECTID IN") then
                return err.set(ERR_NOT_SUPPORTED, 
                               "Sorry, objectId IN is not supported")
            end
            -- end of looking for special tokens

            if not tokenFound then
                print "Done (reached first unknown token)"
                addWhereAndIfNeeded()
                return nParsed
            end

            if string.len(s) < 4 then
                print "Done (no more predicates)"
                return nParsed
            end
           
            if string.byte(s) == 32 then -- skip space
                s = string.sub(s, 2)
                nParsed = nParsed + 1
            end
            if string.find(s, "^AND") then -- remove leading AND
                s = string.sub(s, 5)
                nParsed = nParsed + 4
            elseif string.find(s, "^OR") then
                return err.set(ERR_OR_NOT_ALLOWED, 
                               "'OR' is not allowed here: '"..s.."'")
            else
                return err.set(ERR_AND_EXPECTED, 
                               "'AND' was expected here: '"..s.."'")
            end
        end
        return retV
    end

    ---------------------------------------------------------------------------

    return {
        setAndNeeded = setAndNeeded,
        parseIt = parseIt
    }
end

parser = miniParser()

-------------------------------------------------------------------------------
---- --                          Query type                                  --
-------------------------------------------------------------------------------

function queryType()

    -- Detects if query can be handled locally without sending it to qserv
    local isLocal = function(qU)
        if string.find(qU, "^SELECT @@VERSION_COMMENT LIMIT") or
            string.find(qU, "^SHOW DATABASES") or
            string.find(qU, "^SHOW TABLES") or
            string.find(qU, "^DESCRIBE ") or
            string.find(qU, "^DESC ") then
            return true
        end
        return false
    end

    ---------------------------------------------------------------------------

    local isDisallowed = function(qU)
        if string.find(qU, "^INSERT ") or
           string.find(qU, "^UPDATE ") or
           string.find(qU, "^LOAD ") or
           string.find(qU, "^CREATE ") or
           string.find(qU, "^ALTER ") or
           string.find(qU, "^TRUNCATE ") or
           string.find(qU, "^DROP ") then
            err.set(ERR_NOT_SUPPORTED, 
                    "Sorry, this type of queries is disallowed.")
            return true
        end
        return false
    end

    ---------------------------------------------------------------------------

    local isNotSupported = function(qU)
        if string.find(qU, "^EXPLAIN ") or
           string.find(qU, "^GRANT ") or
           string.find(qU, "^FLUSH ") then 
            err.set(ERR_NOT_SUPPORTED, 
                    "Sorry, this type of queries is not supported in DC3b.")
            return true
        end
        return false
    end

    ---------------------------------------------------------------------------

    return {
        isLocal = isLocal,
        isDisallowed = isDisallowed,
        isNotSupported = isNotSupported
    }
end

qType = queryType()


-------------------------------------------------------------------------------
--                            Query processing                               --
-------------------------------------------------------------------------------

function queryProcessing()

    -- q  - original query
    -- qU - original query but all uppercase
    local sendToQserv = function(q, qU)
        local p1 = string.find(qU, "WHERE")
        if p1 then
            queryToPassStr = string.sub(q, 0, p1-1)

            -- Handle special predicates, modify queryToPassStr as necessary
            local p2 = parser.parseIt(qU, p1+6) -- 6=length of 'where '
            hintsToPassStr = utils.tableToString(hintsToPassArr)
            -- Add all remaining predicates
            if ( p2 < 0 ) then
                return err.send()
            end
            local pEnd = string.len(qU)
            queryToPassStr = queryToPassStr .. ' ' .. string.sub(q, p2, pEnd)
        else
            queryToPassStr = q
            hintsToPassStr = ""
        end

        print ("Passing query: " .. queryToPassStr)
        print ("Passing hints: " .. hintsToPassStr)

        local ok, res = 
           xmlrpc.http.call (rpcHP, "submitQuery", 
                             queryToPassStr, hintsToPassArr)
        if (ok) then
            -- print ("got via rpc " .. res)
            -- for i, v in pairs(res) do print ('\t', i, v) end
        else
            return err.setAndSend(ERR_RPC_CALL, "rpc call failed for " .. rpcHP)
        end
    end

    ---------------------------------------------------------------------------

    local processLocally = function(q)
        -- print ("Processing locally: " .. q)
        print ("Processing locally")
        return 0
    end

    ---------------------------------------------------------------------------

    return {
        sendToQserv = sendToQserv,
        processLocally = processLocally
    }

end

qProc = queryProcessing()


-------------------------------------------------------------------------------
--                           "public" functions                              --
-------------------------------------------------------------------------------

function read_query(packet)
    if string.byte(packet) == proxy.COM_QUERY then
        print("\n*******************\nIntercepted: " .. string.sub(packet, 2))

        -- massage the query string to simplify its processing
        local q = utils.removeExtraWhiteSpaces(string.sub(packet,2))
            -- it is useful to always have a space
            -- even at the end of last predicate
        local qU = string.upper(q) .. ' '

        -- check for special queries that can be handled locally
        if qType.isLocal(qU) then
            return qProc.processLocally(qU)
        end
        -- check for queries that are disallowed
        if qType.isDisallowed(qU) then
            return err.send()
        end
        -- check for queries that we don't support yet
        if qType.isNotSupported(qU) then
            return err.send()
        end

        -- process the query and send it to qserv
        return qProc.sendToQserv(q, qU)
    end
end


