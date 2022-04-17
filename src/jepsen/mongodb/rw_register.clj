(ns jepsen.mongodb.rw-register
    "Elle rw workload"
    (:require [clojure [pprint :refer [pprint]]]
      [clojure.tools.logging :refer [info warn]]
      [clojure.string :as str]
      [dom-top.core :refer [with-retry]]
      [jepsen [client :as client]
       [checker :as checker]
       [independent :as independent]
       [generator :as gen]
       [util :as util :refer [timeout]]]
      [jepsen.tests.cycle :as cycle]
      [jepsen.tests.cycle.wr :as rw-register]
      [jepsen.mongodb [client :as c]
       [list-reify :as listr]]
      [jepsen.checker.timeline :as timeline]
      [knossos.model :as model]
      ;; [jepsen.generator.pure :as gen.pure]
      [slingshot.slingshot :as slingshot])
    (:import (java.util.concurrent TimeUnit)
      (com.mongodb TransactionOptions
                   ServerAddress
                   ReadConcern
                   ReadPreference
                   WriteConcern)
      (org.bson.types ObjectId)
      ;;  (java.util.Arrays Arrays)
      ;;  (knossos.core.Model (clojure.lang PersistentQueue))
      (com.mongodb.client.model Filters
                                UpdateOptions))
    ;; (:import knossos.core.Model
    ;;        (clojure.lang PersistentQueue))
    ;; (:use clojure.tools.logging)

    )

(def db-name   "jepsendb")
(def coll-name "jepsencoll")

(defn txn-options
      "Constructs options for this transaction."
      [test txn]
      ; Transactions retry for well over 100 seconds and I cannot for the life of
      ; me find what switch makes that timeout shorter. MaxCommitTime only affects
      ; a *single* invocation of the transaction, not the retries. We work around
      ; this by timing out in Jepsen as well.
      (cond-> (TransactionOptions/builder)
              true (.maxCommitTime 5 TimeUnit/SECONDS)

              ; MongoDB *ignores* the DB and collection-level read and write concerns
              ; within a transaction, which seems... bad, because it actually
              ; *downgrades* safety if you chose high levels at the db or collection
              ; levels! We have to set them here too.
              (:txn-read-concern test)
              (.readConcern (c/read-concern (:txn-read-concern test)))

              (and (:txn-write-concern test)
                   ; If the transaction is read-only, and we have
                   ; no-read-only-txn-write-concern set, we don't bother setting the write
                   ; concern.
                   (not (and (every? (comp #{:r} first) txn)
                             (:no-read-only-txn-write-concern test))))
              (.writeConcern (c/write-concern (:txn-write-concern test)))

              true .build))

(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 10) (rand-int 10)]})

(defn std-gen
      "Takes a client generator and wraps it in a typical schedule and nemesis
      causing failover."
      [gen]
      (gen/phases
        (->> gen
             (gen/delay 1/20)
             (gen/nemesis
               (cycle [(gen/sleep 10)
                       {:type :info :f :stop}
                       {:type :info :f :start}]))
             (gen/time-limit 120))
        ; Recover
        (gen/nemesis
          (gen/once {:type :info :f :stop}))
        ; Wait for resumption of normal ops
        (gen/clients
          (->> gen
               (gen/delay 1/2)
               (gen/time-limit 10)))))
;; ))

;; (defn apply-mop!
;;   "Applies a transactional micro-operation to a connection."
;;   [test db session [f k v :as mop]]
;;   (let [coll (c/collection db coll-name)]
;;     ;(info (with-out-str
;;     ;        (println "db levels")
;;     ;        (prn :sn-rc ReadConcern/SNAPSHOT)
;;     ;        (prn :ma-rc ReadConcern/MAJORITY)
;;     ;        (prn :db-rc (.getReadConcern db))
;;     ;        (prn :ma-wc WriteConcern/MAJORITY)
;;     ;        (prn :db-wc (.getWriteConcern db))))
;;     (case f
;;       :r      [f k (long (:value (c/find-one coll session k)))]
;;       :w (let [filt (Filters/eq "_id" k)
;;                doc  (c/->doc {:$set {:value v}})
;;                opts (.. (UpdateOptions.) (upsert true))
;;                res  (if session
;;                       (.updateOne coll session filt doc opts)
;;                       (.updateOne coll filt doc opts))]
;;            (info :res res)
;;            mop))))

(defn appendString
      [originalS, appendS, port]
      (str originalS appendS ":" port ","))

(defrecord Client [conn id]
           client/Client
           (open! [this test node]
                  ;; (let [nodelist (:nodes test)
                  ;;       nodelist1 (str/join (str ":" (c/driver-conn-port test) ",") nodelist)
                  ;;       nodelist2 (str "mongodb://" nodelist1)
                  ;;       nodelist3 (str nodelist2 ":" (c/driver-conn-port test))
                  ;;       ]
                  ;;       (def nodelist4 nodelist3)
                  ;; )
                  ;; (info "nodelist:" nodelist4)

                  (assoc this :conn (c/open_replica test node (c/driver-conn-port test))))

           (setup! [this test]
                   (with-retry [tries 5]
                               (let [db   (c/db conn db-name test)]
                                    ; Shard database
                                    ;(c/admin-command! conn {:enableSharding db-name})
                                    (let [coll (c/create-collection! db coll-name)]
                                         (info "Collection created")
                                         ; Shard it!
                                         ;(c/admin-command! conn
                                         ;                  {:shardCollection  (str db-name "." coll-name)
                                         ;                   :key              {:_id :hashed}
                                         ;                   ; WIP; gotta figure out how we're going to
                                         ;                   ; generate queries with the shard key in them.
                                         ;                   ;:key              {(case (:shard-key test)
                                         ;                   ;                     :id :_id
                                         ;                   ;                     :value :value)
                                         ;                   ;                   :hashed}
                                         ;                   :numInitialChunks 7})
                                         (info "Collection not sharded")))
                               (catch com.mongodb.MongoNotPrimaryException e
                                 ; sigh, why is this a thing
                                 nil)
                               (catch com.mongodb.MongoSocketReadTimeoutException e
                                 (if (pos? tries)
                                   (do (info "Timed out sharding DB and collection; waiting to retry")
                                       (Thread/sleep 5000)
                                       (retry (dec tries)))
                                   (throw e))))
                   ; Collections have to be predeclared; transactions can't create them.
                   ;; (with-retry [tries 5]
                   ;;             (let [db   (c/db conn db-name test)]
                   ;;               (if (c/sharded-cluster? test)
                   ;;                 (c/admin-command! conn {:enableSharding db-name}))
                   ;;               (let [coll (c/create-collection! db coll-name)]
                   ;;                 (info "Collection created")
                   ;;                 (if (c/sharded-cluster? test)
                   ;;                   ; Shard it!
                   ;;                   ((c/admin-command! conn
                   ;;                                      {:shardCollection  (str db-name "." coll-name)
                   ;;                                       :key              {:_id :hashed}
                   ;;                                       ; WIP; gotta figure out how we're going to
                   ;;                                       ; generate queries with the shard key in them.
                   ;;                                       ;:key              {(case (:shard-key test)
                   ;;                                       ;                     :id :_id
                   ;;                                       ;                     :value :value)
                   ;;                                       ;                   :hashed}
                   ;;                                       :numInitialChunks 7})
                   ;;                    (info "Collection sharded")))))
                   ;;             (catch com.mongodb.MongoNotPrimaryException e
                   ;;               ; sigh, why is this a thing
                   ;;               nil)
                   ;;             (catch com.mongodb.MongoSocketReadTimeoutException e
                   ;;               (if (pos? tries)
                   ;;                 (do (info "Timed out sharding DB and collection; waiting to retry")
                   ;;                     (Thread/sleep 5000)
                   ;;                     (retry (dec tries)))
                   ;;                 (throw e)))))
                   )
           ;; (invoke! [this test op]
           ;;   (let [txn (:value op)]
           ;;     (c/with-errors op
           ;;       (timeout 5000 (assoc op :type :info, :error :timeout)
           ;;         (let [txn' (let [db (c/db conn db-name test)]
           ;;           (with-open [session (c/start-session conn)]
           ;;             (let [opts (txn-options test (:value op))
           ;;                   body (c/txn
           ;;                     ;(info :txn-begins)
           ;; (mapv (partial apply-mop!
           ;;                test db session)
           ;;      (:value op)))]
           ;;               (.withTransaction session body opts))))]
           ;;     (assoc op :type :ok, :value txn'))))))

           ;; (defn apply-mop!
           ;;   "Applies a transactional micro-operation to a connection."
           ;;   [test db session [f k v :as mop]]
           ;;   (let [coll (c/collection db coll-name)]
           ;;     ;(info (with-out-str
           ;;     ;        (println "db levels")
           ;;     ;        (prn :sn-rc ReadConcern/SNAPSHOT)
           ;;     ;        (prn :ma-rc ReadConcern/MAJORITY)
           ;;     ;        (prn :db-rc (.getReadConcern db))
           ;;     ;        (prn :ma-wc WriteConcern/MAJORITY)
           ;;     ;        (prn :db-wc (.getWriteConcern db))))
           ;;     (case f
           ;; :r      [f k (long (:value (c/find-one coll session k)))]
           ;;       :w (let [filt (Filters/eq "_id" k)
           ;;                doc  (c/->doc {:$set {:value v}})
           ;;                opts (.. (UpdateOptions.) (upsert true))
           ;;                res  (if session
           ;;                       (.updateOne coll session filt doc opts)
           ;;                       (.updateOne coll filt doc opts))]
           ;;            (info :res res)
           ;;            mop))))

           (invoke!
             [this test op]
             (c/with-errors op
                            (timeout 5000 (assoc op :type :info, :error :timeout)
                                     (let [db (c/db conn db-name test)]
                                          (let [coll (c/collection db coll-name)]
                                               (with-open [session (c/start-session conn)]
                                                          (case (:f op)
                                                                :read
                                                                ;This line is set for linearizable read (do not use session!)
                                                                (assoc op :type :ok, :value (if (:value (c/find-one coll session id))
                                                                                              (long (:value (c/find-one coll session id)))
                                                                                              233))
                                                                :write (let [filt (Filters/eq "key-" id)
                                                                             doc (c/->doc {:$push {:value (:value op)}})
                                                                             opts (.. (UpdateOptions.) (upsert true))
                                                                             ;如果需要的是unacknoeledged的write concern，则也不能在session中进行
                                                                             res (if session
                                                                                   (.updateOne coll session filt doc opts)
                                                                                   (.updateOne coll filt doc opts))]
                                                                            (assoc op :type :ok))
                                                                :cas  (let [[old new] (:value op)
                                                                            ;; filt (Filters/and (Arrays/asList (Filters/eq "_id" id) (Filters/eq "value" old)))
                                                                            filt (Filters/eq "value" old)
                                                                            doc (c/->doc {:$set {:value new}})
                                                                            ;一定要设置upsert false，否则会插入不相干数据
                                                                            opts (.. (UpdateOptions.) (upsert false))
                                                                            res (if session
                                                                                  (.updateOne coll session filt doc opts)
                                                                                  (.updateOne coll filt doc opts))]
                                                                           (condp = (.getModifiedCount res)
                                                                                  0 (assoc op :type :fail)
                                                                                  1 (assoc op :type :ok)
                                                                                  2 (throw (ex-info "CAS unexpected number of modified docs"
                                                                                                    {:n (.getModifiedCount res)
                                                                                                     :res res}))
                                                                                  )
                                                                           ;; :cas (let [filt (Filters/eq "_id" id)
                                                                           ;;             [value value'] (:value op)
                                                                           ;;             doc (c/->doc {:$set {:value value'}})
                                                                           ;;             opts (.. (UpdateOptions.) (upsert true))
                                                                           ;;             res (if session
                                                                           ;;                   (.updateOne coll session filt doc opts)
                                                                           ;;                   (.updateOne coll filt doc opts))]
                                                                           ;;         (assoc op :type :ok))
                                                                           ;; (condp = (.getN res)
                                                                           ;;   0 (assoc op :type :fail)
                                                                           ;;   1 (assoc op :type :ok)
                                                                           ;;   2 (throw (ex-info "CAS unexpected number of modified docs"
                                                                           ;;                {:n (.getN res)
                                                                           ;;                 :res res})))
                                                                           ;; ((assoc op :type :ok))
                                                                           )
                                                                )))))))


           (teardown! [this test])

           (close! [this test]
                   (.close conn)
                   (info "Close worker...")
                   ))

(defn myclient
      "A client which implements a register on top of an entire document."
      [write-concern]
      (Client.  nil
                "key"))

(defn rw-test
      [opts]
      {
       ;; :model     (cas-register)
       ;; :checker   (checker/compose {:linear checker/linearizable})
       ; :checker         (checker/compose
       ;                           {:perf   (checker/perf)
       ;                           :indep (independent/checker
       ;                                   (checker/compose
       ;                                    {:linear   (checker/linearizable
       ;                                                {:model (model/cas-register)
       ;                                                :algorithm :linear})
       ;                                  :timeline (timeline/html)}))})
       :checker   (checker/linearizable
                    {:model     (model/cas-register)
                     :algorithm :linear})
       ;; :checker   (rw-register/checker opts)
       ;; :model     (model/cas-register)
       ;; :checker   (checker/compose {:linear checker/linearizable})
       ;;  :generator (independent/concurrent-generator
       ;;               10
       ;;               (range)
       ;;               (fn [k]
       ;;                 (->> (gen/mix [r w])
       ;;                      (gen/limit (:ops-per-key opts)))))
       ;注意：如果需要指定write concern级别为unack时，只能生成r和w，因为cas操作无法从unack的写中返回有效信息
       :generator (std-gen (gen/mix [r w]))
       ;:generator (let [n (count (:nodes opts))]
       ;                (independent/concurrent-generator
       ;                  n
       ;                  (range)
       ;                  (fn [k]
       ;                      (cond->> (gen/reserve n r (gen/mix [r w]))
       ;                               ; We randomize the limit a bit so that over time, keys
       ;                               ; become misaligned, which prevents us from lining up
       ;                               ; on Significant Event Boundaries.
       ;                               (:per-key-limit opts)
       ;                               (gen/limit (* (+ (rand 0.1) 0.9)
       ;                                             (:per-key-limit opts 20)))
       ;
       ;                               true
       ;                               (gen/process-limit (:process-limit opts 20))))))

       ;:generator (let [gen (gen/mix [r w])]
       ;                (gen/phases
       ;                  (->> gen
       ;                       (gen/delay 1/20)
       ;                       (gen/time-limit 120))
       ;                  ; Wait for resumption of normal ops
       ;                  (gen/clients
       ;                    (->> gen
       ;                         (gen/delay 1/2)
       ;                         (gen/time-limit 10))))
       ;             )


       })

(defn workload
      "A generator, client, and checker for a rw-register test."
      [opts]
      (assoc (rw-test {:nodes (:nodes opts)
                       :key-count          10
                       :key-dist           :exponential
                       :max-txn-length     (:max-txn-length opts 4)
                       :max-writes-per-key (:max-writes-per-key opts)
                       :consistency-models [:strong-snapshot-isolation]})
             :client (myclient WriteConcern/UNACKNOWLEDGED)))