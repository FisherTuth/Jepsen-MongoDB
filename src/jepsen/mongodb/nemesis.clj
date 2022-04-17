(ns jepsen.mongodb.nemesis
  "Nemeses for MongoDB"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [nemesis :as n]
                    [net :as net]
                    [control :as c]
                    [util :as util :refer [majority
                                           random-nonempty-subset]]
                    [db :as jdb]]
            [jepsen.generator :as gen]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]
            [jepsen.mongodb.db :as db]))

(def min-cluster-size
  "How small can the cluster get?"
  1)
(defn shard-generator
  "Takes a collection of shard packages, and returns a generator that emits ops
  like {:f whatever, :shard \"foo\", :value blah}, drawn from one of the shard
  packages."
  [packages]
  (->> packages
       (map (fn [pkg]
              (let [shard-name (:name pkg)]
                (gen/map (fn [op] (assoc op :shard shard-name))
                         (:generator pkg)))))
       gen/mix))

(defn shard-nemesis
  "Takes a collection of shard packages, and returns a nemesis that
  takes ops like {:f whatever, :shard \"foo\", :value blah}, and dispatches
  that op to the nemesis for that particular shard."
  [packages]
  (reify n/Nemesis
    (setup! [this test]
      (shard-nemesis
        (real-pmap (fn [pkg]
                     (update pkg :nemesis
                             n/setup! (db/test-for-shard test pkg)))
                   packages)))

    (invoke! [this test op]
      (let [shard-name (:shard op)
            pkg        (first (filter (comp #{shard-name} :name) packages))
            nemesis    (:nemesis pkg)
            test       (db/test-for-shard test pkg)
            op'        (n/invoke! (:nemesis pkg) test op)]
        op'))

    (teardown! [this test]
      (real-pmap (fn [pkg]
                   (n/teardown! (:nemesis pkg)
                                (db/test-for-shard test pkg)))
                 packages))

    n/Reflection
    (fs [this]
      (set (mapcat (comp n/fs :nemesis) packages)))))

(defn member-nemesis
      "A nemesis for adding and removing nodes from the cluster. Options:

        :db     The database to grow and shrink.

      Leave operations can either be a node name, or a map of {:remove node :using
      node}, in which case we ask a specific node to remove the other."
      [opts]
      (reify n/Nemesis
             (setup! [this test] this)

             (invoke! [this test op]
                      (info "In nemsis current membership is\n" (with-out-str
                                                     (pprint (db/get-meta-members (:db opts)))
                                                     (pprint (remove (set (jdb/primaries (:db opts) test)) (db/members (:db opts) test)))
                                                     (pprint (map (fn [member] {:name (:name member),:stateStr (:stateStr member)} ) (:members (db/node-state test))))))
                      (assoc op :value
                             (case (:f op)
                                   :hold   nil
                                   :join   (if
                                             (< 0 (count (remove (set (db/members (:db opts) test)) (:nodes test))))
                                             (let [test2 (assoc test :nodes (db/members (:db opts) test))
                                                   target
                                                   (case (:value op)
                                                         :one (rand-nth (remove (set (:nodes test2)) (:nodes test)))
                                                         :all nil
                                                         )]
                                                  (case (:value op)
                                                      :one [(:value op) (db/join!  (:db opts) test target)]
                                                       :all [(:value op) (db/join-all!  (:db opts) test)]
                                                    )
                                                  )
                                             nil
                                             )
                                   :leave  (if
                                             (< 4 (count (db/members (:db opts) test)))
                                             (let [test2 (assoc test :nodes (db/members (:db opts) test))
                                                   nodes (remove (set (jdb/primaries (:db opts) test2)) (:nodes test2))
                                                   target
                                                   (case (:value op)
                                                         :one (rand-nth nodes)
                                                         :majority (take (quot (+ 2 (count nodes)) 2) nodes)
                                                         )]
                                                  (case (:value op)
                                                        :one [(:value op) (db/leave! (:db opts) test target)]
                                                        :majority [(:value op) (db/leave-majority! (:db opts) test target)]
                                                        )
                                                  )
                                             nil
                                             )
                                   :election (if
                                                (< 1 (count (db/members (:db opts) test)))
                                               [(:value op) (db/election! (:db opts) test)]
                                               nil
                                               )
                                   )))
             ;[grudge (grudge test db (:value op))]

             (teardown! [this test] this)

             n/Reflection
             (fs [this] [:join :leave :hold :election])))
(defn db-nodes
      "Takes a test, a DB, and a node specification. Returns a collection of
      nodes taken from that test. node-spec may be one of:

         nil              - Chooses a random, non-empty subset of nodes
         :one             - Chooses a single random node
         :minority        - Chooses a random minority of nodes
         :majority        - Chooses a random majority of nodes
         :primaries       - A random nonempty subset of nodes which we think are
                            primaries
         :all             - All nodes
         [\"a\", ...]     - The specified nodes"
      [test db node-spec]
      (let [nodes (:nodes test)]
           (case node-spec
                 nil         (random-nonempty-subset nodes)
                 :one        (list (rand-nth nodes))
                 :minority   (take (dec (majority (count nodes))) (shuffle nodes))
                 :majority   (take      (majority (count nodes))  (shuffle nodes))
                 :primaries  (jdb/primaries db test)
                 :all        nodes
                 :none       nil
                 node-spec)))
(defn member-nemesis-force
      "A nemesis which can perform various DB-specific operations on nodes. Takes a
  database to operate on. This nemesis responds to the following f's:

     :start
     :kill
     :pause
     :resume

  In all cases, the :value is a node spec, as interpreted by db-nodes."
      [dbk opts]
      (reify
        n/Reflection
        (fs [this] [:join :leave :hold :election])

        n/Nemesis
        (setup! [this test] this)

        (invoke! [this test op]
                 (if
                   (not= :hold (:f op))
                   (let [f (case (:f op)
                                 :leave   jdb/pause!
                                 :join  jdb/resume!
                                 :election jdb/pause!)
                         nodes (case (:f op)
                                 :leave  (db-nodes test dbk (:value op))
                                 :join   (db-nodes test dbk (:value op))
                                 :election (db-nodes test dbk :primaries)
                                 )
                         res (c/on-nodes test nodes (partial f dbk))]
                        (assoc op :value res))
                   (assoc op :value :done)
                   )

                 )

        (teardown! [this test])))
(defn random-sublist
      "Randomly drops elements from the given collection."
      [coll]
      (filter (fn [_] (< 0.6 (rand))) coll))
(defn join-leave-gen
      "Emits join and leave operations for a DB."
      [db shard]
      (println "===============" "this is join-leave-gen")
        (if-not (= "config" (:name shard))
          (let [members (set
                          ;(db/members db shard)
                          (:nodes shard)
                          )
                addable (remove members (:nodes shard))
                ]
               ;(cond ; We can add someone
               ;  (and (seq addable) (< (rand) 0.5))
               ;  {:type :info, :f :join, :value (rand-nth (vec addable))}
               ;
               ;  ; We can remove someone
               ;  (< min-cluster-size (count members))
               ;  {:type :info, :f :leave, :value (rand-nth (vec members))}
               ;
               ;  ; Huh, no options at all.
               ;  true
               ;  {:type :info, :f :hold, :value {:type :can't-change
               ;                                  :members members
               ;                                  :nodes (:nodes shard)}})
               ;(vec
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :join, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :join, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :join, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))}
               ; {:type :info, :f :leave, :value (rand-nth (vec members))})
               (->> [
                      :leave :one
                     :leave :one
                     :join     :one
                     :join     :one
                     :leave :one
                     :leave :one
                     :join     :one
                     :join     :one
                     ;:hold     :none
                     ;:hold     :none
                     ;:hold     :none
                     ;:hold     :none
                     ;:hold     :none
                     ;:hold     :none
                     ;:join     :one
                     ;:hold     :none
                     ;:leave    :majority
                     ;:join     :all
                     ;:leave    :majority
                     ;:join     :all
                     ;:hold     :none
                     ;:leave    :one
                     ;:join     :one
                     ;:hold     :none
                     ;:election :none
                     ;:join     :all
                     ;:hold     :none
                     ;:leave    :majority
                     ;:hold     :none
                     ;:join     :one
                     ;:join     :all
                     ;:hold     :none
                     ;:election :none
                     ;:leave    :one
                     ;:leave    :one
                     ;:join     :one
                     ;:hold     :none
                     ;:election :none
                     ;:join     :one
                     ;:join     :one
                     ;:leave  :one
                     ;:leave  :one
                     ;:leave  "n8"
                     ;:leave  "n9"
                     ;:leave  "n10"
                     ;:leave  "n7"
                     ;:join   "n7"
                     ;:leave  "n8"
                     ;:leave  "n9"
                     ;:leave  "n7"
                     ;:leave  "n8"
                     ;:join   "n7"
                     ;:leave  "n7"
                     ;:leave  "n7"
                     ]
                    (partition 2)
                    cycle
                    random-sublist
                    (map (fn [[f v]] {:type :info, :f f, :value v}))
               )
          )

        )
      )
(defn member-generator
      "Generates join and leave operations. Options:

        :db         The DB to act on.
        :interval   How long to wait between operations."
      [opts shard]
      ; Feels like the generator should be smarter here, and take over choosing
      ; nodes.
      (->> (join-leave-gen (:db opts) shard)
           (gen/delay (:interval opts))))
(defn member-package
      "A membership generator and nemesis. Options:

        :interval   How long to wait between operations.
        :db         The database to add/remove nodes from.
        :faults     The set of faults. Should include :member to activate this
                    package."
      [opts shard]
      (when ((:faults opts) :member)
            {:nemesis   (if (= (:m-type opts) 1) (member-nemesis-force (:db opts) opts)(member-nemesis opts))
             :generator (member-generator opts shard)
             :perf      #{{:name  "join"
                           :fs    [:join]
                           :color "#E9A0E6"}
                          {:name  "leave"
                           :fs    [:leave]
                           :color "#ACA0E9"}}}))
(defn package-for
      "Builds a combined package for the given options."
      [opts shard]
      (println "=================" "this is shard in package-for" shard)
      (->> (nc/nemesis-packages opts)
           (concat [(member-package opts shard)])
           (remove nil?)
           nc/compose-packages))

(defn package-for-shard
      "Builds a nemesis package for a specific shard, merged with the shard map
      itself."
      [opts shard]
      (println "=================asdasdasdasds" "this is opts in package-for-shard" opts)
      (println "====================sada=asddd" "this is shard in package-for-shard" shard)
      (merge shard
             (package-for (assoc opts :db (:db shard)) shard)))
(defn nemesis-package
  "Constructs a nemesis and generators for MongoDB."
  [opts]
  (let [opts (update opts :faults set)
        nemesis-opts (cond (some #{:island} (:faults opts))
                           (update opts :faults conj :member :partition)
                           (some #{:mystery} (:faults opts))
                           (update opts :faults conj :member :kill)
                           true opts)
        ; Construct a package for each shard
        pkgs (map (partial package-for-shard nemesis-opts) (:shards (:db nemesis-opts)))


        ]
    ;(println "=================sadsad=sa=====================sad" "this is pkgs in nemesis-package"(take 5 (pkgs)))
    ;(assert false)
    ; Now, we need a generator and nemesis which mix operations on various
    ; shards, and route those operations to the nemesis for each appropriate
    ; shards. We merge these onto a nemesis package for the whole test--that
    ; gives us

    (assoc (nc/nemesis-package opts)
           :generator        (shard-generator pkgs)
           :final-generator  nil
           :nemesis          (shard-nemesis pkgs)))
    ; Or just do a standard package
    ; TODO: mix these
    ;(nc/nemesis-package)
    )
