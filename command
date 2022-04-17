{:_id "rs_shard1",:configsvr false,:settings{:heartbeatTimeoutSecs 1,:electionTimeoutMillis 1000,:catchUpTimeoutMillis 1000,:catchUpTakeoverDelayMillis 3000},:members({:_id 0, :priority 5, :host "n6:27018"}{:_id 1, :priority 4, :host "n7:27018"}{:_id 2, :priority 3, :host "n8:27018"}{:_id 3, :priority 2, :host "n9:27018"}{:_id 4, :priority 1, :host "n10:27018"})}

db.runCommand({"replSetInitiate":{"_id":"kevinDemo","members":[{"_id":1,"host":"127.0.0.1:27017","priority":1},{"_id":2,"host":"127.0.0.1:27217","priority":2},{"_id":3,"host":"127.0.0.1:27317","priority":3}]}})
