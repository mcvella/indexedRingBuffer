local indexedRingBuffer = {_TYPE='module', _NAME='indexedRingBuffer' }

local dictCache = require "dictCache"
local localCache = require "cache"
local cjson = require "cjson"
cjson.encode_sparse_array(false,0,0)

local ID_SEP = ';;'


function indexedRingBuffer.new( params )

    local shared = localCache
    -- if we are running in nginx, use shared dict caches
    if ngx then
        shared = dictCache
    end

    local self = {
        cache = shared.new("ringBuffer"),
        cacheIndex = shared.new("ringBufferIndex"),
        sizeStats = shared.new("sizeStats"),
        autoResize = params.autoResize or false,
        desiredEjectMins = params.desiredEjectMins or 15,
        autoMinSize = params.autoMinSize or 10000,
        autoMaxSize = params.autoMaxSize or 10000000,
        monitorPeriodMins = params.monitorPeriodMins or 10,
        triggerAdjustPercent = params.triggerAdjustPercent or 20,
        maxAdjustPercentUp = params.maxAdjustPercent or 25,
        maxAdjustPercentDown = params.maxAdjustPercent or 10,
        paramList = params.paramList,
        storageMap = {},
        storageInitString = '{}',
        ngxEjectUpstream = params.ngxEjectUpstream or "/ejectItem_upstream",
        drainParallelItems = params.drainParallelItems or 100,
        ejectFunction = params.ejectFunction
    }

    function self.drain()
        -- only one drain can occur at a given time
        if not self.cache:get( "draining" ) then
            self.cache:set( "draining", true )
            local pos = 1
            local item = self.cache:get(1)
            local callList = {}
            while item do
                -- if ngx is present, we can leverage upstream for faster parallel drain
                if ngx and self.ngxEjectUpstream then
                    table.insert( callList, { self.ngxEjectUpstream, { args = "pos=" .. pos }  } )

                    if #callList >= self.drainParallelItems then
                        local res = ngx.location.capture_multi( callList )
                        callList = {}
                    end
                else
                    self.ejectItem(pos)
                end

                pos = pos + 1
                item = self.cache:get(pos)
            end

            if ngx and #callList > 0 then
                local res = ngx.location.capture_multi( callList )
            end

            self.cache:flush_all()
            self.cache:flush_expired()
            self.cacheIndex:flush_all()
            self.cacheIndex:flush_expired()
            -- reinitialize position
            self.cache:set( "pos", 0 )
            self.cache:set( "draining", false )
        end
    end

    function self.ejectItem( itemPos, doDel )
        local item = self.cache:get(itemPos)
        local splitVal = splitString(item, ID_SEP)
        if self.ejectFunction then
            self.ejectFunction(splitVal[1], self.makeReadableParams(cjson.decode(splitVal[2])), true)
        end

        if doDel then
            self.cacheIndex:delete( splitVal[1] )
            self.cache:delete( itemPos )
        end
    end

    function self.resize(size)
        local prevSize = self.sizeStats:get( "currentSize" )

        self.sizeStats:set( "currentSize", size )

        if size < prevSize then
            -- if we are currently higher than new max, move to end of new size and eject the rest
            if self.cache:get( "pos" ) > size then
                self.cache:set( "pos", size )
            end

            local pos = size + 1
            local item = self.cache:get(pos)
            local callList = {}
            while item do
                -- if ngx is present, we can leverage upstream for faster parallel drain
                if ngx and self.ngxEjectUpstream then
                   table.insert( callList, { self.ngxEjectUpstream, { args = "del=1&pos=" .. pos }  } )

                   if #callList >= self.drainParallelItems then
                       local res = ngx.location.capture_multi( callList )
                       callList = {}
                   end
                else
                    self.ejectItem(pos, true)
                end

                pos = pos + 1
                item = self.cache:get(pos)
            end

            if ngx and #callList > 0 then
                local res = ngx.location.capture_multi( callList )
            end
        end
    end

    function self.stats()
        local serverStart = self.sizeStats:get( "serverStart" )
        local totalReqCount = self.sizeStats:get( "totalReqCount" )
        local totalItemCount = self.sizeStats:get( "totalItemCount" )

        local stats = {
            currentSize = self.sizeStats:get( "currentSize" ),
            totalReqCount = totalReqCount,
            avgReqsSec = totalReqCount / ( os.time() - serverStart ),
            totalItemCount = totalItemCount,
            avgItemsSec = totalItemCount / ( os.time() - serverStart ),
            serverStart = os.date("!%Y-%m-%dT%TZ", serverStart),
            lastPeriodAvgMins = self.sizeStats:get( "lastPeriodAvgMins" ),
            draining = self.cache:get( "draining" ) or false
        }
        return stats
    end

    function self.set(id, params)
        -- do not do anything if a drain is in process
        if self.cache:get( "draining" ) then
            return
        end

        -- first, see if this item already exists
        local currentItemPos = self.cacheIndex:get( id )
        local currentVal

        if currentItemPos then
            currentVal = self.cache:get( currentItemPos )
            if not currentVal then
                 self.cacheIndex:delete( id )
            end
        end

        -- if this item does not currently exist, add new item, and process evicted item, if any
        if not currentVal then
            currentItemPos = self.cache:incr( "pos", 1 )

            --ngx.log( ngx.DEBUG, currentItemPos .. " " .. self.sizeStats:get( "currentSize" ) )

            if ( currentItemPos > self.sizeStats:get( "currentSize" ) ) then
                --ngx.log( ngx.DEBUG, "Reached max size of " .. self.sizeStats:get( "currentSize" ) .. ", will start at pos 1" )
                self.cache:set( "pos", 1 )
                currentItemPos = 1
            end

            local existingItem = self.cache:get( currentItemPos )

            if existingItem then

                local spVal = splitString( existingItem, ID_SEP )

                if self.ejectFunction then
                    --ngx.log( ngx.DEBUG, "Will eject " .. spVal[1] )
                    self.ejectFunction( spVal[1], self.makeReadableParams(cjson.decode(spVal[2])), false )
                end

                self.cacheIndex:delete( spVal[1] )
                self.cache:delete( currentItemPos )
            end

            self.cacheIndex:set( id, currentItemPos )
        end

        ngx.log( ngx.DEBUG, "Will set " .. id .. " at " .. currentItemPos )

        local newVal = self.mergeValWithParams(id, currentVal, params)
        local success, err = self.cache:set( currentItemPos, newVal )

        -- track cache rate and resize if needed
        self.checkRate(currentVal)

        return
    end

    function self.mergeValWithParams(id, currentVal, params)
        local current

         if not currentVal then
            currentVal = self.storageInitString
         else
            local spCurr = splitString(currentVal, ID_SEP)
            currentVal = spCurr[2]
         end

         current = cjson.decode( currentVal )

         for name, val in pairs(params) do
            if self.storageMap[name] and val ~= '' then
                if type(val) == 'string' and string.find(val, ',') then
                    val = '"'..val..'"'
                end
                current[self.storageMap[name]] = val
            end
         end
        return id .. ID_SEP .. cjson.encode( current )
    end

    function self.makeReadableParams(params)
        local t = {}
        for name, val in pairs(self.storageMap) do
            if params[val] then
                t[name] = params[val]
            end 
        end
        return t
    end

    function self.initStorage( paramList )

        local storageMap = {}
        local storageInit = {}

        for aIndex = 1, #paramList do
          storageMap[paramList[aIndex].input] = tostring(aIndex)
        end

        self.storageInitString = cjson.encode(storageInit)
        self.storageMap = storageMap

        -- initial draining status
        self.cache:set( "draining", false )
    end

    function self.initSizeStats( reinit )
        self.sizeStats:set( "itemCount", 0 )
        self.sizeStats:set( 'periodStart', os.time() )
        self.sizeStats:delete( 'locked' )

        if not reinit then
            self.sizeStats:set( "totalReqCount", 0 )
            self.sizeStats:set( "totalItemCount", 0 )
            self.sizeStats:set( "serverStart", os.time() )
        end
    end

    function self.checkRate(currentItem)
        -- if this is just an update to a current item, don't track new stat or consider resizing
        if not currentItem then
            if self.autoResize then
                local locked = self.sizeStats:get( "locked" )
                if not locked then
                    -- is it time to determine rate?
                    local startSampleTime = self.sizeStats:get( 'periodStart' )

                    if (os.time() - startSampleTime) > ( self.monitorPeriodMins * 60 )
                    then
                        -- monitor period has passed. lock and see if we should adjust
                        local locked = self.sizeStats:add( "locked", true )
                        if locked then
                            local count = self.sizeStats:get( "itemCount" )
                            if count then
                                local avgEjectMins = ( self.sizeStats:get( "currentSize" ) / count ) * self.monitorPeriodMins
                                self.sizeStats:set( "lastPeriodAvgMins", avgEjectMins )

                                -- is avg eject minutes not within acceptable range of desired?
                                if  ( math.abs( 1 - (avgEjectMins / self.desiredEjectMins) ) * 100 ) > self.triggerAdjustPercent then
                                     --ngx.log( ngx.WARN, "Adjust cache size: YES, desired mins is " .. self.desiredEjectMins .. " and we are ejecting at " .. avgEjectMins )

                                     local desiredCacheSize =  ( count / self.monitorPeriodMins ) * self.desiredEjectMins
                                     local newCacheSize = desiredCacheSize

                                     -- make sure we don't change more than x% at once
                                     local diffPercent = (desiredCacheSize - self.sizeStats:get( "currentSize" )) / self.sizeStats:get( "currentSize" )

                                     -- we can move up at a different rate than we move down
                                     local maxAdjustPercent = self.maxAdjustPercentUp
                                     if diffPercent < 0 then
                                        maxAdjustPercent = self.maxAdjustPercentDown
                                     end

                                     if math.abs(diffPercent) * 100 > maxAdjustPercent then

                                        local maxChange = math.floor( self.sizeStats:get( "currentSize" ) * ( maxAdjustPercent / 100 ) )
                                        if diffPercent > 0 then
                                            newCacheSize = self.sizeStats:get( "currentSize" ) + maxChange
                                        else
                                            newCacheSize = self.sizeStats:get( "currentSize" ) - maxChange
                                        end

                                     end

                                     -- make sure we don't go over or under the min/max cache sizes
                                     if newCacheSize > self.autoMaxSize then
                                        newCacheSize = self.autoMaxSize
                                     end

                                     if newCacheSize < self.autoMinSize then
                                        newCacheSize = self.autoMinSize
                                     end

                                     --ngx.log( ngx.WARN, "Desired cache size: " .. desiredCacheSize .. ", new size: " .. newCacheSize )

                                     -- resize!
                                     self.resize(newCacheSize)
                                else
                                    --ngx.log( ngx.WARN, "Adjust cache size: NO, desired mins is " .. self.desiredEjectMins .. " and we are ejecting at " .. avgEjectMins )
                                end
                            end
                            -- reinitialize (includes unlock)
                            self.initSizeStats(true)
                        end
                    else
                        self.sizeStats:incr( "itemCount", 1 )
                    end
                end
            end
            self.sizeStats:incr( "totalItemCount", 1 )
        end

        self.sizeStats:incr( "totalReqCount", 1 )
    end

    function self.get(id)
        local offset = self.cacheIndex:get(id)
        if not offset then
            return nil
        end
        
        local docWithKey = self.cache:get(offset)
        if docWithKey then
            local doc = cjson.decode(splitString(docWithKey, ID_SEP)[2])
            return cjson.encode(self.makeReadableParams(doc))
        end
    end

    -- initialize size
    self.sizeStats:set( "currentSize", params.initialSize or 1000000 )

    -- initialize position
    self.cache:set( "pos", 0 )

    -- initialize storage template
    self.initStorage( self.paramList )

    -- initialize size stats
    self.initSizeStats()

    return self

end

function splitString(s, pattern)
    if not pattern then pattern = "%s+" end
    local ret = {}
    local pos = 1
    local fstart, fend = string.find(s, pattern, pos)
    while fstart do
        table.insert(ret, string.sub(s, pos, fstart - 1))
        pos = fend + 1
        fstart, fend = string.find(s, pattern, pos)
    end
    table.insert(ret, string.sub(s, pos))
    return ret
end

return indexedRingBuffer
