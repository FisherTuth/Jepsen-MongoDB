(ns jepsen.mongodb.db
  "Database setup and automation."
  (:require [taoensso.carmine :as car :refer [wcar]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [dom-top.core :refer [with-retry]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c ]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh random-nonempty-subset timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb [client :as client]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def log-file "/var/log/mongodb/mongod.log")
(def data-dir "/var/lib/mongodb")

(def mongos-dir "/tmp/mongos")
(def mongos-log-file "/var/log/mongodb/mongos.stdout")
(def mongos-pid-file (str mongos-dir "/mongos.pid"))
(def mongos-bin "mongos")

(def subpackages
  "MongoDB has like five different packages to install; these are the ones we
  want."
  ["mongos"
   "server"])

(defprotocol Membership
             "Allows a database to support node introspection, growing, and shrinking, the
             cluster. What a mess."
             (get-meta-members [db]       "Local DB membership state machine.")
             (members  [db test]      "The set of nodes currently in the cluster.")
             (election! [db test]     "Trigger an election for the shard")
             (join!    [db test node] "Add a node to the cluster.")
             (join-all!    [db test] "Add a node to the cluster.")
             (leave!   [db test node] "Removes a node from the cluster.")
             (leave-majority!   [db test nodes] "Removes a node from the cluster.")
             (start!!    [db test node] "start!! a node in the cluster.")
             (stop!!    [db test node] "kill!! a node in cluster.")
             )

(defprotocol Health
             "Allows a database to signal when a node is alive."
             (up? [db test node]))
(defn on-some-primary
      "Evaluates (f test node) on some (randomly ordered) primary node in the DB,
      trying each primary in turn until no exception is thrown."
      [db test f]
      (with-retry [nodes (shuffle (db/primaries db test))]
                  (when (seq nodes)
                        (c/on-nodes test (take 1 nodes) f))
                  (catch Exception e
                    (if-let [more (next nodes)]
                            (retry more)
                            (throw e)))))


(defn deb-url
  "What's the URL of the Debian package we install?"
  [test subpackage]
  (let [version (:version test)]
    ; TODO: sort out the 4.2 in the URL here
    (str "https://repo.mongodb.org/apt/debian/dists/buster/mongodb-org/4.2/main/binary-amd64/mongodb-org-" subpackage "_" version "_amd64.deb")))

(defn install!
  [test]
  "Installs MongoDB on the current node."
  (c/su
    (c/exec :mkdir :-p "/tmp/jepsen")
    (c/cd "/tmp/jepsen"
          (doseq [subpackage subpackages]
            (when-not (= (:version test)
                         (debian/installed-version (str "mongodb-org-"
                                                        subpackage)))
              (let [file (cu/wget! (deb-url test subpackage))]
                (info "Installing" subpackage (:version test))
                (c/exec :dpkg :-i :--force-confnew file))
              (c/exec :systemctl :daemon-reload))))))

(defn config-server?
  "Takes a test map, and returns true iff this set of nodes is intended to be a
  configsvr."
  [test]
  (= (:replica-set-name test) "rs_config"))

(defn shard-for-node
      "Takes a sharded DB and a node; returns the shard this node belongs to."
      [sharded-db node]
      (first (filter (fn [shard] (some #{node} (:nodes shard)))
                     (:shards sharded-db))))

(defn node-state
      "This is "
      [test]
      (let [shard (shard-for-node (:db test) (first (:nodes test)))

            primary (first (db/primaries (:db shard) test))
            ;primary (first (:nodes test))
            port (if (config-server? test)
                   client/config-port
                   (client/shard-port test))]
           ;(println "==============" "this is test-nodes in node-state" (:nodes test))
           ;(println "=================" "this is sharded-db" (:db test))
           ;(println "=================" "this is shards" (:shards (:db test)))
           ;(println "=================" "this is node" (first (:nodes test)))
           ;(println "=================" "this is shard" shard)
           (with-open [conn (client/open primary port)]
                      (client/admin-command! conn {:replSetGetStatus 1})
                      )
           )
      )

(defn await-node-removal
      "Blocks until node id no longer appears in the local (presumably, leader)'s
      node set."
      [test node]

      (let [r
            ;(try+
            ;    (raft-info)
            ;        (catch [:exit 1] e
            ;          :retry)
            ;        (catch Throwable e
            ;          (warn e "Crash fetching raft-info")
            ;          :retry))
            ;:retry
            (map :name (:members (node-state test)))
            ]
           (if
             ;(or (= :retry r)
             ;      (= id (:node_id (:raft r)))
             ;      (some #{id} (map :id (:nodes (:raft r)))))
             ;false
             (some #{node} r)
             (do (info :waiting-for-removal node)
                 (pprint r)
                 (Thread/sleep 1000)
                 (recur test node))
             (do ;(info :done-waiting-for-removal id
               ;(with-out-str (pprint r)))
               :done))))

(defn await-node-join
      "Blocks until node id no longer appears in the local (presumably, leader)'s
      node set."
      [test node time]

      (let [
            r
            ;(try+
            ;    (raft-info)
            ;        (catch [:exit 1] e
            ;          :retry)
            ;        (catch Throwable e
            ;          (warn e "Crash fetching raft-info")
            ;          :retry))
            ;:retry
            (map (fn [node] {:name (:name node) :stateStr (:stateStr node)}) (:members (node-state test)))
            ]
           (if (> 5 time)
             (if
               (and
                 (some #{(str node ":" (if (config-server? test)
                                         client/config-port
                                         (client/shard-port test)))} (map (fn [node] (:name node)) r))
                 (or
                   (= "SECONDARY" (:stateStr (first (filter (comp #{(str node ":" (if (config-server? test)
                                                                                    client/config-port
                                                                                    (client/shard-port test)))} :name) r))))
                   (= "PRIMARY" (:stateStr (first (filter (comp #{(str node ":" (if (config-server? test)
                                                                                  client/config-port
                                                                                  (client/shard-port test)))} :name) r))))
                   )
                 )
               (do ;(info :done-waiting-for-removal id
                 ;(with-out-str (pprint r)))
                 :done)
               (do (info :waiting-for-joining node)
                   (pprint r)
                   (pprint (str node ":" (if (config-server? test)
                                           client/config-port
                                           (client/shard-port test))))
                   (Thread/sleep 1000)
                   (recur test node (+ 1 time)))
               )

             :failed
             )
           ))
(defn await-node-join-all
      "Blocks until node id no longer appears in the local (presumably, leader)'s
      node set."
      [test time]

      (let [
            count1 (atom 0)
            r
            (map (fn [node] {:name (:name node) :stateStr (:stateStr node)}) (:members (node-state test)))
            ]
           (if (> 6 time)
             (do
               (doseq [node r]
                      (if
                        (or
                          (= "PRIMARY" (:stateStr node))
                          (= "SECONDARY" (:stateStr node))
                          )
                        (swap! count1 inc)
                        )
                      )
               (if
                 (= @count1 (count r)
                    )
                 (do ;(info :done-waiting-for-removal id
                   ;(with-out-str (pprint r)))
                   :done)
                 (do (info :waiting-for-joining :all)
                     (pprint r)

                     (Thread/sleep 1000)
                     (recur test (+ 1 time)))
                 )

               )

             :failed
             )
           ))
(defn await-node-election
      "Blocks until node id no longer appears in the local (presumably, leader)'s
      node set."
      [test]

      (let [r
            (map (fn [node] {:name (:name node) :stateStr (:stateStr node)}) (:members (node-state test)))
            ]
           (if
             (and
               (< 0 (count (filter (comp #{"PRIMARY"} :stateStr) r)))
               )
             (do ;(info :done-waiting-for-removal id
               ;(with-out-str (pprint r)))
               :done)
             (do (info :waiting-for-election (:replica-set-name test))
                 (pprint r)
                 (Thread/sleep 1000)
                 (recur test))
             )))
(defn await-node-stop
      "Blocks until node id no longer appears in the local (presumably, leader)'s
      node set."
      [test node time]

      (let [
            r
            ;(try+
            ;    (raft-info)
            ;        (catch [:exit 1] e
            ;          :retry)
            ;        (catch Throwable e
            ;          (warn e "Crash fetching raft-info")
            ;          :retry))
            ;:retry
            (map (fn [node] {:name (:name node) :stateStr (:stateStr node)}) (:members (node-state test)))
            ]
           (if (> 5 time)
             (if
               (and
                 (some #{(str node ":" (if (config-server? test)
                                         client/config-port
                                         (client/shard-port test)))} (map (fn [node] (:name node)) r))
                 (not
                   (or
                     (= "SECONDARY" (:stateStr (first (filter (comp #{(str node ":" (if (config-server? test)
                                                                                      client/config-port
                                                                                      (client/shard-port test)))} :name) r))))
                     (= "PRIMARY" (:stateStr (first (filter (comp #{(str node ":" (if (config-server? test)
                                                                                    client/config-port
                                                                                    (client/shard-port test)))} :name) r))))
                     )
                   )
                 )
               (do ;(info :done-waiting-for-removal id
                 ;(with-out-str (pprint r)))
                 :done)
               (do (info :waiting-for-pausing node)
                   (pprint r)
                   (pprint (str node ":" (if (config-server? test)
                                           client/config-port
                                           (client/shard-port test))))
                   (Thread/sleep 1000)
                   (recur test node (+ 1 time)))
               )

             :failed
             )
           ))
(defn replica-set-deployment?
      [test]
      (= (:deployment test) :replica-set))
(defn configure!
  "Sets up configuration files"
  [test node]
      (info "configuring" node)
  (c/su
    (if (replica-set-deployment? test)
      (c/exec :echo :> "/etc/mongod.conf"
              (-> (do
                    (info "is in replica set mode...")
                    (slurp (io/resource "replica-set/mongod.conf"))

                    )
                  (str/replace "%REPL_SET_NAME%"
                               (:replica-set-name test "rs_jepsen")
                               )

                  )

              )

      (c/exec :echo :> "/etc/mongod.conf"
              (-> (slurp (io/resource "mongod.conf"))
                  (str/replace "%REPL_SET_NAME%"
                               (:replica-set-name test "rs_jepsen"))
                  (str/replace "%CLUSTER_ROLE%"
                               (if (config-server? test)
                                 "configsvr"
                                 "shardsvr")))))))

(defn start!
  "Starts mongod"
  [test node]
  (info "Starting mongod")
  (c/su (c/exec :systemctl :start :mongod)))

(defn stop!
  "Stops the mongodb service"
  [test node]
  (info "Stopping mongod")
  (try+
    (c/su (c/exec :systemctl :stop :mongod))
    (catch [:exit 5] e
      ; Not loaded; we probably haven't installed
      )))

(defn wipe!
  "Removes logs and data files"
  [test node]
  (c/su (c/exec :rm :-rf log-file (c/lit (str data-dir "/*")))))

;; Replica sets

(defn target-replica-set-config
  "Generates the config for a replset in a given test."
  [test]
  {:_id (:replica-set-name test "rs_jepsen")
   :configsvr (config-server? test)
   ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
   :settings {:heartbeatTimeoutSecs       1
              :electionTimeoutMillis      1000
              :catchUpTimeoutMillis       1000
              :catchUpTakeoverDelayMillis 3000}
   :version 1
   :members (->> test
                 :nodes
                 (map-indexed (fn [i node]
                                {:_id  i
                                 :priority (- (count (:nodes test)) i)
                                 :host     (str node ":"
                                                (if (config-server? test)
                                                  client/config-port
                                                  (client/shard-port test)))})))})

(defn target-replica-set-reConfigTest
      "Generates the config for a replset in a given test."
      [test node member version]
      {:_id (:replica-set-name test "rs_jepsen")
       :configsvr (config-server? test)
       ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
       :settings {:heartbeatTimeoutSecs       1
                  :electionTimeoutMillis      1000
                  :catchUpTimeoutMillis       1000
                  :catchUpTakeoverDelayMillis 3000}
       :version version
       :members (->> test
                     :nodes
                     (map-indexed (fn [i nodex]
                                      (when (and
                                                  (not= node nodex)
                                                  (some #{nodex} member)
                                                  )
                                                              {:_id  i
                                                               :priority (- (count (:nodes test)) i)
                                                               :host     (str nodex ":"
                                                                              (if (config-server? test)
                                                                                client/config-port
                                                                                (client/shard-port test)))
                                                               })
                                      ))
                     (remove nil?)
                     )})
(defn target-replica-set-reConfigTest2
      "Generates the config for a replset in a given test."
      [test node member version]
      {:_id (:replica-set-name test "rs_jepsen")
       :configsvr (config-server? test)
       ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
       :settings {:heartbeatTimeoutSecs       1
                  :electionTimeoutMillis      1000
                  :catchUpTimeoutMillis       1000
                  :catchUpTakeoverDelayMillis 3000}
       :version version
       :members (->> test
                     :nodes
                     (map-indexed (fn [i nodex]
                                      (when (or
                                              (= node nodex)
                                              (some #{nodex} member)
                                              )
                                            {:_id  i
                                             :priority (- (count (:nodes test)) i)
                                             :host     (str nodex ":"
                                                            (if (config-server? test)
                                                              client/config-port
                                                              (client/shard-port test)))
                                             })
                                      ))
                     (remove nil?)
                     )})
(defn target-replica-set-reConfigTest3
      "Generates the config for a replset in a given test."
      [test nodes member version]
      {:_id (:replica-set-name test "rs_jepsen")
       :configsvr (config-server? test)
       ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
       :settings {:heartbeatTimeoutSecs       1
                  :electionTimeoutMillis      1000
                  :catchUpTimeoutMillis       1000
                  :catchUpTakeoverDelayMillis 3000}
       :version version
       :members (->> test
                     :nodes
                     (map-indexed (fn [i nodex]
                                      (when (and
                                              (not (some #{nodex} nodes))
                                              (some #{nodex} member)
                                              )
                                            {:_id  i
                                             :priority (- (count (:nodes test)) i)
                                             :host     (str nodex ":"
                                                            (if (config-server? test)
                                                              client/config-port
                                                              (client/shard-port test)))
                                             })
                                      ))
                     (remove nil?)
                     )})
(defn target-replica-set-configTest4
      "Generates the config for a replset in a given test."
      [test version]
      {:_id (:replica-set-name test "rs_jepsen")
       :configsvr (config-server? test)
       ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
       :settings {:heartbeatTimeoutSecs       1
                  :electionTimeoutMillis      1000
                  :catchUpTimeoutMillis       1000
                  :catchUpTakeoverDelayMillis 3000}
       :version version
       :members (->> test
                     :nodes
                     (map-indexed (fn [i node]
                                      {:_id  i
                                       :priority (- (count (:nodes test)) i)
                                       :host     (str node ":"
                                                      (if (config-server? test)
                                                        client/config-port
                                                        (client/shard-port test)))})))})
(defn replica-set-initiate!
  "Initialize a replica set on a node."
  [conn config]
  (client/admin-command! conn {:replSetInitiate config}))
;我复制的
(defn replica-set-reInitiateTest!
      "Initialize a replica set on a node."
      [conn config]
      (client/admin-command! conn {:replSetReconfig  config }))

(defn replica-set-config
  "Returns the current repl set config"
  [conn]
  (client/admin-command! conn {:replSetGetConfig 1}))

(defn replica-set-status
  "Returns the current replica set status."
  [conn]
  (client/admin-command! conn {:replSetGetStatus 1}))

(defn replica-step-down-primary
      "Returns the current replica set status."
      [conn]
      (client/admin-command! conn {:replSetStepDown 10}))

(defn primaries
  "What nodes does this conn think are primaries?"
  [conn]
  (->> (replica-set-status conn)
       :members
       (filter #(= "PRIMARY" (:stateStr %)))
       (map :name)
       (map client/addr->node)))

(defn primary
  "Which single node does this conn think the primary is? Throws for multiple
  primaries, cuz that sounds like a fun and interesting bug, haha."
  [conn]
  (let [ps (primaries conn)]
    (when (< 1 (count ps))
      (throw (IllegalStateException.
               (str "Multiple primaries known to "
                    conn
                    ": "
                    ps))))

    (first ps)))

(defn await-join
  "Block until all nodes in the test are known to this connection's replset
  status"
  [test conn]
  (while (not= (set (:nodes test))
               (->> (replica-set-status conn)
                    :members
                    (map :name)
                    (map client/addr->node)
                    set))
    (info :replica-set-status
          (with-out-str (->> (replica-set-status conn)
                                                 :members
                                                 (map :name)
                                                 (map client/addr->node)
                                                 sort
                                                 pprint)
                        (prn :test (sort (:nodes test)))))
    (Thread/sleep 1000)))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn initial-join!
  "Joins nodes into a replica set. Intended for use during setup."
  [test node]
  (let [port (if (config-server? test)
               client/config-port
               (client/shard-port test)
               )]
    ; Wait for all nodes to be reachable
    (.close (client/await-open node port))
    (jepsen/synchronize test 300)

    ; Start RS
    (when (= node (jepsen/primary test))
      (with-open [conn (client/open node port)]
        (info "Initiating replica set on" node "\n"
              (with-out-str (pprint (target-replica-set-config test))))
                 ;这里我做了改动
                 (replica-set-initiate! conn (target-replica-set-config test))

        (info "Waiting for cluster join")
                 (await-join test conn)

        (info "Waiting for primary election")
                 (await-primary conn)
        (info "Primary ready")))

    ; For reasons I really don't understand, you have to prevent other nodes
    ; from checking the replset status until *after* we initiate the replset on
    ; the primary--so we insert a barrier here to make sure other nodes don't
    ; wait until primary initiation is complete.
    (jepsen/synchronize test 300)

    ; For other reasons I don't understand, you *have* to open a new set of
    ; connections after replset initation. I have a hunch that this happens
    ; because of a deadlock or something in mongodb itself, but it could also
    ; be a client connection-closing-detection bug.

    ; Amusingly, we can't just time out these operations; the client appears to
    ; swallow thread interrupts and keep on doing, well, something. FML.
    (with-open [conn (client/open node port)]
      (info "Waiting for cluster join")
      (await-join test conn)

      (info "Waiting for primary")
      (await-primary conn)

      (info "Primary is" (primary conn))
      (jepsen/synchronize test 300))
       ))
(def config-version (atom 1))
(defn replica-set-db
  "This database runs a single replica set."
  []
  (let [meta-members (atom {})]
       (reify
         db/DB
         (setup! [db test node]
                 (install! test)
                 (configure! test node)
                 (start! test node)
                 (initial-join! test node)
                 (swap! meta-members assoc node {:state :live}))

         (teardown! [db test node]
                    (stop! test node)
                    (wipe! test node))

         db/LogFiles
         (log-files [db test node]
                    [log-file])

         db/Process
         (start! [_ test node]
                 (start! test node)
                 ;(Thread/sleep 1000)
                 (swap! meta-members assoc node {:state :live})
                 )

         (kill! [_ test node]
                (c/su (cu/grepkill! :mongod))
                ;(Thread/sleep 1000)
                (swap! meta-members assoc node {:state :dead})
                (stop! test node))

         db/Pause
         (pause! [_ test node]
                 (swap! meta-members assoc node {:state :dead})
                 (c/su (cu/grepkill! :stop :mongod))
                  ;(await-node-stop test node 1)
                 )

         (resume! [_ test node]
                  (swap! meta-members assoc node {:state :live})
                  (c/su (cu/grepkill! :cont :mongod))

                  )

         db/Primary
         (setup-primary! [_ test node])

         (primaries [_ test]
                    (let [test (assoc test :nodes (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))]
                         (println "================" "this is nodes for primaries" (:nodes test))
                         (try (->> (:nodes test)
                                   (real-pmap (fn [node]
                                                  (with-open [conn (client/open
                                                                     node
                                                                     (if (config-server? test)
                                                                       client/config-port
                                                                       (client/shard-port test)))]
                                                             ; Huh, sometimes Mongodb DOES return multiple
                                                             ; primaries from a single request. Weeeeird.
                                                             (primaries conn))))
                                   (reduce concat)
                                   distinct)
                              (catch Exception e
                                (info e "Can't determine current primaries")
                                nil))
                      ))
         Membership
         (get-meta-members [db] @meta-members)
         (members [db test]
                  (->> (node-state test)
                       :members
                       (map :name)
                       (map client/addr->node)
                       (remove (->> @meta-members
                                    (filter (comp #{:dead} :state val))
                                    (map key)
                                    set))))

         (election! [db test]
                    (let [primary (first (db/primaries db test))
                          res (with-open [conn (client/open
                                                 ;node
                                                 primary
                                                 (if (config-server? test)
                                                   client/config-port
                                                   (client/shard-port test)))]
                                         ; Huh, sometimes Mongodb DOES return multiple
                                         ; primaries from a single request. Weeeeird.
                                         (println "========================" "test:node is " (:nodes test))
                                         (pprint @meta-members)
                                         (println "========================" "live is " (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))
                                         (:ok (replica-step-down-primary conn))
                                         )]
                         (await-node-election test)
                         (info "Current membership\n" (with-out-str
                                                        (pprint @meta-members)
                                                        (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)} ) (:members (node-state test))))))
                      res))
         (leave! [db test node-or-map]
                 (let [[node primary] (if (map? node-or-map)
                                        [(:remove node-or-map) (:using node-or-map)]
                                        [node-or-map nil])
                       ;id  (node-id test node)
                       res (do
                             (info (:replica-set-name test) :removing node)
                             (let [m (swap! meta-members update-in [node :state]
                                            {:dead      :dead
                                             :joining   :joining
                                             :live      :removing
                                             :removing  :removing})]
                                  (when-not (= :removing (get-in m [node :state]))
                                            (throw+ {:type  :can't-remove-in-this-state
                                                     :node  node
                                                     :members m}))
                                  (let [
                                        ;res (cli! "RAFT.NODE" "REMOVE" id)
                                        res (try
                                              (with-open [conn (client/open
                                                                 ;node
                                                                 (first (db/primaries db test))
                                                                 (if (config-server? test)
                                                                   client/config-port
                                                                   (client/shard-port test)))]
                                                         ; Huh, sometimes Mongodb DOES return multiple
                                                         ; primaries from a single request. Weeeeird.
                                                         (do
                                                           (swap! config-version inc)

                                                           (:ok (->>
                                                                  @config-version
                                                                  (target-replica-set-reConfigTest test node (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))
                                                                  (replica-set-reInitiateTest! conn)))

                                                           )
                                                         )
                                              (catch Exception e
                                                (info e "leave failed")
                                                nil)
                                              )

                                        ;res "OK"
                                        ]
                                       (when
                                         (= 1.0 res)
                                         (do
                                           (locking meta-members
                                                    (swap! meta-members assoc-in [node :state] :dead)
                                                    (swap! meta-members assoc node {:state :dead})
                                                    )
                                           (Thread/sleep 1000)
                                           )
                                         )

                                       (info "Current membership\n" (with-out-str
                                                                      (pprint @meta-members)
                                                                      (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)} ) (:members (node-state test))))))
                                       res))



                             )]
                      (or res :no-primary-available)))

         (leave-majority! [db test nodes]
                          (let [
                                ;id  (node-id test node)
                                res (do
                                      (info (:replica-set-name test) :removing nodes)
                                      (doseq [node nodes]
                                        (swap! meta-members update-in [node :state]
                                               {:dead      :dead
                                                :joining   :joining
                                                :live      :removing
                                                :removing  :removing})
                                        )
                                      (let [
                                            ;res (cli! "RAFT.NODE" "REMOVE" id)
                                            res (try
                                                  (with-open [conn (client/open
                                                                     ;node
                                                                     (first (db/primaries db test))
                                                                     (if (config-server? test)
                                                                       client/config-port
                                                                       (client/shard-port test)))]
                                                             ; Huh, sometimes Mongodb DOES return multiple
                                                             ; primaries from a single request. Weeeeird.
                                                             (do
                                                               (swap! config-version inc)
                                                               (println "========================" "primary is " (first (db/primaries db test)))
                                                               (println "========================" "version is " @config-version)
                                                               (println "========================" "test:node is " (:nodes test))
                                                               (pprint @meta-members)
                                                               (println "========================" "live is " (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))
                                                               (pprint (target-replica-set-reConfigTest3 test nodes (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)) @config-version))
                                                               (:ok (->>
                                                                      @config-version
                                                                      (target-replica-set-reConfigTest3 test nodes (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))
                                                                      (replica-set-reInitiateTest! conn)))

                                                               )
                                                             )
                                                  (catch Exception e
                                                    (info e "leave failed")
                                                    nil)
                                                  )

                                            ;res "OK"
                                            ]
                                           (when
                                             (= 1.0 res)
                                             (do
                                               (locking meta-members
                                                        (doseq [node nodes]
                                                          (swap! meta-members assoc-in [node :state] :dead)
                                                          (swap! meta-members assoc node {:state :dead})
                                                          )
                                                        )
                                               (info "Killing and wiping" nodes)
                                               (Thread/sleep 1000)
                                               )
                                             )

                                           (info "Current membership\n" (with-out-str
                                                                          (pprint @meta-members)
                                                                          (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)} ) (:members (node-state test))))))
                                           res)



                                      )]
                               (or res :no-primary-available)))

         (join! [db test node]
                (let [
                      primary (first (db/primaries db test))
                      m (swap! meta-members update-in [node :state]
                               {:dead     :joining      ; We can join a dead node
                                :joining  :joining      ; We can try to join again
                                :live     :live         ; But we can't join a live node
                                :removing :removing})]  ; Or one being removed
                     ; Are we joining? If not, abort here.
                     (when-not (= :joining (get-in m [node :state]))
                               (throw+ {:type    :can't-join-in-this-state
                                        :node    node
                                        :members m}))
                     ; Good, let's go.
                     (let [res (try
                                 (with-open [conn (client/open
                                                    ;node
                                                    (first (db/primaries db test))
                                                    (if (config-server? test)
                                                      client/config-port
                                                      (client/shard-port test)))]
                                            (do
                                              (println "================ will start")
                                              (Thread/sleep 1000)
                                              (info node :joining primary)
                                              (swap! config-version inc)
                                              (:ok (->>
                                                     @config-version
                                                     (target-replica-set-reConfigTest2 test node (map (fn [member] (first member)) (filter (comp #{:live} :state val) @meta-members)))
                                                     (replica-set-reInitiateTest! conn)))
                                              )
                                            )
                                 (catch Exception e
                                   (info e "leave failed")
                                   nil))]
                          (let [r (await-node-join test node 1) ]
                               (if (= :done r)
                                 (do; And mark that the join completed.
                                  (swap! meta-members assoc-in [node :state] :live)))
                                 )
                          (info "Current membership\n" (with-out-str
                                                         (pprint @meta-members)
                                                         (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)}) (:members (node-state test))))))
                          res)))
         (join-all! [db test]

                    (let [
                          primary (first (db/primaries db test))
                          m (doseq [node (:nodes test)]
                              (swap! meta-members update-in [node :state]
                                     {:dead     :joining      ; We can join a dead node
                                      :joining  :joining      ; We can try to join again
                                      :live     :live         ; But we can't join a live node
                                      :removing :removing})
                              )
                          ]  ; Or one being removed
                         ; Are we joining? If not, abort here.
                         ;(when-not (= :joining (get-in m [node :state]))
                         ;          (throw+ {:type    :can't-join-in-this-state
                         ;                   :node    node
                         ;                   :members m}))
                         ; Good, let's go.
                         (let [res (try
                                     (with-open [conn (client/open
                                                        ;node
                                                        (first (db/primaries db test))
                                                        (if (config-server? test)
                                                          client/config-port
                                                          (client/shard-port test)))]
                                                (do
                                                  (println "================ will start")
                                                  (Thread/sleep 1000)
                                                  (info  :joining primary)
                                                  (swap! config-version inc)
                                                  (:ok (->>
                                                         @config-version
                                                         (target-replica-set-configTest4 test  )
                                                         (replica-set-reInitiateTest! conn)))
                                                  )
                                                )
                                     (catch Exception e
                                       (info e "leave failed")
                                       nil))]
                              (try
                                ;(db/setup! db test node)
                                (catch Exception e
                                  (info e "leave failed")
                                  nil))
                              (let [r (await-node-join-all test 1) ]
                                   (if (= :done r)
                                     (do; And mark that the join completed.
                                      (doseq [node (:nodes test)]
                                             (swap! meta-members assoc-in [node :state] :live)
                                             )
                                       ))
                                   )
                              (info "Current membership\n" (with-out-str
                                                             (pprint @meta-members)
                                                             (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)}) (:members (node-state test))))))
                              res))


                    )
         (start!! [db test node]
                  (c/on-nodes test [node] (fn start [_ _]
                                              (c/su (cu/grepkill! :cont :mongod))
                                              (Thread/sleep 1000)
                                              (info :starting node "!!!!!")
                                              ))
                  (swap! meta-members assoc node {:state :live})
           )
         (stop!! [db test node]
                 (c/on-nodes test [node] (fn start [_ _]
                                             (do
                                               (try+
                                                 (c/su (cu/grepkill! :stop :mongod))
                                                 (catch [:exit 5] e
                                                   ; Not loaded; we probably haven't installed
                                                   ))
                                               (Thread/sleep 1000)
                                               (info :stopping node "!!!!!")
                                               )
                                             ))
                  (swap! meta-members assoc node {:state :dead})
                  )
         )
    )
  )

;; Sharding

(defn shard-node-plan
  "Takes a test, and produces a map of shard names to lists of nodes
  which form the replica set for that set. We always generate a config replica
  set, and fill remaining nodes with shards.

    {\"config\" [\"n1\" \"n2\" ...]
     \"shard1\" [\"n4\" ...]
     \"shard2\" [\"n7\" ...]}"
  [test]
  (let [n           (:nodes test)
        shard-size  5]
    (println "===========================" n  (count n))
    (assert (<= (* 2 shard-size) (count n))
            (str "Need at least " (* 2 shard-size) " nodes for 1 shard"))
    (zipmap (->> (range) (map inc) (map (partial str "shard")) (cons "config"))
            (partition-all shard-size n))))

(defn test-for-shard
  "Takes a test map and a shard map, and creates a version of the test map with
  the replica set name and nodes based on the given shard.

  (test-for-shard test {:nodes [...})"
  [test shard]
  (assoc test
         :nodes             (:nodes shard)
         :replica-set-name  (str "rs_" (:name shard))))



(defn on-shards
  "Takes a sharded DB. Calls (f shard) in parallel on each
  shard. Returns a map of shard names to the results of f on that shard."
  [sharded-db f]
  (zipmap (map :name (:shards sharded-db))
          (real-pmap f (:shards sharded-db))))

(defn on-shards-nodes
  "Takes a sharded DB. Calls (f shard node) in parallel on each shard and node.
  Returns a map of shards to nodes to the results of f on that shard and node."
  [sharded-db f]
  (on-shards (fn [shard]
               (zipmap (:nodes shard)
                       (real-pmap (partial f shard) (:nodes shard))))))

(defn configure-mongos!
  "Sets up mongos configuration file."
  [test node config-db]
  (c/su
    (c/exec :echo :> "/etc/mongos.conf"
            (-> (slurp (io/resource "mongos.conf"))
                (str/replace "%CONFIG_DB%" config-db)))))

(defn start-mongos!
  "Starts the mongos daemon on the local node."
  [test node]
  (c/su
    (c/exec :mkdir :-p mongos-dir)
    (cu/start-daemon!
      {:logfile mongos-log-file
       :pidfile mongos-pid-file
       :chdir   mongos-dir}
      (str "/usr/bin/" mongos-bin)
      :--config "/etc/mongos.conf")))

(defn stop-mongos!
  "Stops the mongos daemon on the local node."
  [test node]
  (c/su (cu/stop-daemon! mongos-bin mongos-pid-file)))

(defn add-shards!
  "Adds the initial set of shards for the DB setup."
  [node shard-strs]
  (with-open [conn (client/open node client/mongos-port)]
    (doseq [shard shard-strs]
      (info "Adding shard" shard)
      (client/admin-command! conn {:addShard shard}))))

(defrecord Mongos [config-str shard-strs]
  db/DB
  (setup! [this test node]
    (install! test)
    (configure-mongos! test node config-str)
    (start-mongos! test node)
    (info "Waiting for mongos to start")
    (client/await-open node client/mongos-port)
    (jepsen/synchronize test)
    (when (= (jepsen/primary test) node)
      (add-shards! node shard-strs)))

  (teardown! [this test node]
    (stop-mongos! test node)
    (c/su
      (c/exec :rm :-rf mongos-log-file mongos-dir)))

  db/LogFiles
  (log-files [this test node]
    [mongos-log-file]))

(defrecord ShardedDB [mongos shards tcpdump]
  db/DB
  (setup! [this test node]
    (db/setup! tcpdump test node)
    (let [shard (shard-for-node this node)]
      (info "Setting up shard" shard)
      (db/setup! (:db shard) (test-for-shard test shard) node))

    (db/setup! mongos test node)
          ;(try (with-open [conn (client/open node 27018)]
          ;          (info node (type node))
          ;          (if (= "n6" node) (info "extra" (map :name (:members (client/admin-command! conn {:replSetGetStatus 1}))) ))
          ;          (info  (map :name (:members (client/admin-command! conn {:replSetGetStatus 1}))))
          ;          )
          ;     (catch java.lang.InterruptedException e
          ;       false)
          ;     (catch java.net.SocketTimeoutException e
          ;       false) ; Probably?
          ;     (catch java.net.ConnectException e
          ;       false))
          ;(if
          ;  (= "n6" node)
          ;  ;true
          ;  (let [conn (client/open node 27018)]
          ;       (info node (type node))
          ;       (info "=========" "n6 starting delete n10")
          ;       ;(info "======" (client/admin-command! conn {:replSetStepDown 0}))
          ;       ;(client/admin-command! conn {:replSetReconfig {:members
          ;       ;                                               ({:_id 0, :priority 5, :host "n6:27018"}
          ;       ;                                                {:_id 1, :priority 4, :host "n7:27018"}
          ;       ;                                                {:_id 2, :priority 3, :host "n8:27018"}
          ;       ;                                                {:_id 3, :priority 2, :host "n9:27018"}
          ;       ;                                                )}})
          ;       ;(client/admin-command! conn {:replSetReconfig "/etc/mongod.conf"})
          ;       (info "======" (client/admin-command! conn {:isMaster 1}))
          ;       (info "======" "n6 starting delete n10 succeed!!!!")
          ;  )
          ;       )

          ;(try (with-open [conn (client/open node 27018)]
          ;                (info node (type node))
          ;                (if (= "n6" node) (info "extra" (map :name (:members (client/admin-command! conn {:replSetGetStatus 1}))) ))
          ;                (info  (map :name (:members (client/admin-command! conn {:replSetGetStatus 1}))))
          ;                )
          ;     (catch java.lang.InterruptedException e
          ;       false)
          ;     (catch java.net.SocketTimeoutException e
          ;       false) ; Probably?
          ;     (catch java.net.ConnectException e
          ;       false))
   ;(info "I'm into while" node)
   ;       (with-open [conn (client/open node 27018)]
   ;                  (while true (when (= "n11" node) (info (:members (client/admin-command! conn {:replSetGetStatus 1}))))))
   )

  (teardown! [this test node]
    (db/teardown! mongos test node)
    (let [shard (shard-for-node this node)]
      (info "Tearing down shard" shard)
      (db/teardown! (:db shard) (test-for-shard test shard) node))
    (db/teardown! tcpdump test node))

  db/LogFiles
  (log-files [this test node]
    (concat (db/log-files tcpdump test node)
            (db/log-files mongos test node)
            (let [shard (shard-for-node this node)]
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
      (db/resume! (:db shard) (test-for-shard test shard) node)))


)

(defn sharded-db
  "This database deploys a config server replica set, shard replica sets, and
  mongos sharding servers."
  [opts]
  (let [plan (shard-node-plan opts)]
    (println "==============asdsadasdsadsadsa""this is plan in shard-db" plan)
    (ShardedDB.
      (Mongos.
        ; Config server
        (->> (get plan "config")
             (map #(str % ":" client/config-port))
             (str/join ",")
             (str "rs_config/"))
        ; Shards
        (->> plan
             (keep (fn [[rs nodes]]
                     (when-not (= "config" rs)
                       (str "rs_" rs "/"
                            (first nodes) ":" (client/shard-port test)))))))
      (->> plan
           (map (fn [[shard-name nodes]]
                  {:name  shard-name
                   :nodes nodes
                   :db    (replica-set-db)})))

      (db/tcpdump {:filter "host 192.168.122.1"
                   :ports  [client/mongos-port]})

      )))
