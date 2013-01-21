(defproject SuperWebAnalytics "0.1"
  :min-lein-version "2.0.0"
  :description "From Nathan Marz book Big Data."
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options" "-g"]
  :resource-paths ["src/resources"]
  :thrift-source-path "src/thrift"
  :thrift-java-path "src/java"
  :thrift-opts "beans,nocamel,hashcode"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.apache.thrift/libthrift "0.9.0" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.hadoop/hadoop-core "0.20.2" :exclusions [junit commons-codec]]
                 [backtype/dfs-datastores-cascading "1.2.1"]
                 [cascalog "1.10.0" :exclusion [cascading/cascading-hadoop]]
                 [com.clearspring.analytics/stream "2.2.0"]]
  :profiles {:dev { :dependencies [[org.testng/testng "6.8"]
                                   [org.easymock/easymock "3.1"]
                                   [midje "1.4.0"]]}}
  :plugins [[lein-thrift "0.1.2"]])
