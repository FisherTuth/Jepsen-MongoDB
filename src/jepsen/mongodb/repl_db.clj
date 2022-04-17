(ns jepsen.mongodb.repl-db
    "Database setup and automation for replica set"
    (:require [clojure [pprint :refer [pprint]]]
      [clojure.tools.logging :refer [info warn]]
      [jepsen [db :as db]]
      [jepsen.mongodb [db :refer :all]]))


(defn replica-set-node-plan
      "Takes a test, and produces a lists of nodes which form the replica set for the set.
      Compared to the *shard-node-plan*, we return only a list for the replica set deployment.

      {\"replica\" [\"n1\" \"n2\" ...]}"
      [test]
      (let [n           (:nodes test)
            shard-size  (count n)]
           (zipmap (->> (range) (map inc) (map (partial str "shard")))
                   (partition-all shard-size n))))

(defrecord ReplicaDB [shards]
           db/DB
           (setup! [this test node]
                   (let [shard (shard-for-node this node)]                 ;; A shard is also a replica set
                        (info "Setting up replica" shard)
                        (db/setup! (:db shard) (test-for-shard test shard) node)))

           (teardown! [this test node]
                      (let [shard (shard-for-node this node)]
                           (info "Tearing down replica" shard)
                           (db/teardown! (:db shard) (test-for-shard test shard) node)))

           db/LogFiles
           (log-files [this test node]
                      (concat (let [shard (shard-for-node this node)
                                    _ (info "shard is " shard)]
                                   (db/log-files (:db shard) (test-for-shard test shard) node))))

           db/Primary
           (setup-primary! [_ test node] nil)
           (primaries [this test]
                      (->> (on-shards this
                                      (fn [shard]
                                          (db/primaries (:db shard)
                                                        (test-for-shard test shard))))
                           vals
                           (reduce concat)
                           distinct))

           db/Process
           (start! [this test node]
                   (let [shard (shard-for-node this node)]
                        (db/start! (:db shard) (test-for-shard test shard) node)))

           (kill! [this test node]
                  (let [shard (shard-for-node this node)]
                       (db/kill! (:db shard) (test-for-shard test shard) node)))

           db/Pause
           (pause! [this test node]
                   (let [shard (shard-for-node this node)]
                        (db/pause! (:db shard) (test-for-shard test shard) node)))

           (resume! [this test node]
                    (let [shard (shard-for-node this node)]
                         (db/resume! (:db shard) (test-for-shard test shard) node))))



(defn replica-db
      "This database deploys a replica set deployment"
      [opts]
      (let [plan (replica-set-node-plan opts)]
           (ReplicaDB.
             (->> plan
                  (map (fn [[shard-name nodes]]
                           {:name  shard-name
                            :nodes nodes
                            :db    (replica-set-db)}))))))