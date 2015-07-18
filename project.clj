(defproject mlas "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/data.json "0.2.6"]
                 [gorillalabs/sparkling "1.2.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [snowball-stemmer "0.1.0"]]
  :aot [clojure.tools.logging.impl sparkling.serialization sparkling.destructuring]
  :main ^:skip-aot mlas.core
  :target-path "target/%s"
  :resource-paths ["resources"]
  :profiles {:dev
             {:dependencies [[criterium "0.4.3"]
                             [junit "4.11"]]}
             :provided
             {:dependencies
              [[junit "4.11"]
               [org.apache.hadoop/hadoop-client "2.4.0"]
               [org.apache.spark/spark-core_2.10 "1.4.0"]
               [org.apache.spark/spark-mllib_2.10 "1.3.0"]]
              :aot [mlas.core]}
             :uberjar
             {:aot :all}}
  :jvm-opts ["-Duser.timezone=UTC"
             "-XX:MaxPermSize=256m"
             "-Xmx3G"
             "-XX:+CMSClassUnloadingEnabled"
             "-XX:+UseCompressedOops"
             "-XX:+HeapDumpOnOutOfMemoryError"]




)
