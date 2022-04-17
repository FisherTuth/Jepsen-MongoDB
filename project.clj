(defproject jepsen.mongodb "0.3.1-SNAPSHOT"
  :description "Jepsen MongoDB tests"
  :url "http://github.com/jepsen-io/mongodb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1"]
                 [com.taoensso/carmine "2.19.1"]
                 [org.mongodb/mongodb-driver-sync "4.0.2"]]
  :repositories [
                 ["central" "https://maven.aliyun.com/nexus/content/groups/public"]
                 ["clojars" "https://mirrors.tuna.tsinghua.edu.cn/clojars"]
                 ]
  :main jepsen.mongodb
  :jvm-opts ["-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.mongodb})