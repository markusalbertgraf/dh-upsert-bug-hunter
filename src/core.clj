(ns core
  (:require
   [clojure.java.io :as io]
   [datahike.api :as d]
   [taoensso.timbre :as log]))

"Hunt down a bug in datahike upsert.

  This namespace provides:

  a) get-first-vulnerable-db-size
     to create a database that is vulnerable to the upsert error.
     This is done by creating incrementally bloated schema until the db
     is big enough to display the unwanted behaviour.

     When a database is found it is exported to file db-error

     See the rich comment at the end and just hit eval.

  b) a test that displays the error
     The function upsert-test carries out a number of transactions and
     then queries for what it just transacted.

     There are actually two different errors.  Increase the value you
     pass to autotest to get both.

     Autotest prints Error: true true if both are present.

  c) Just eval the lines in the comment at the end.

  Note: The size to create a db is the size of the bloat schema.
        The eid of the data will be two higher."

;; Needed so big import doesn't kill the cider-repl
(log/merge-config! {:level     :debug
                    :min-level [["datahike.*" :warn]]})

(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d (d/datoms db :eavt)]
        (prn d)))))

(defn update-max-tx-from-file
  "Find bigest tx in file and update max-tx of db.
  Note: the last tx might not be the biggest if the db
  has been imported before."
  [db file]
  (let [max-tx (->> (line-seq (io/reader file))
                    (map read-string)
                    (reduce #(max %1 (nth %2 3)) 0))]
    (assoc db :max-tx max-tx)))

(defn import-db
  "Import a flat-file of datoms at path into your database."
  [conn path]
  (println "Prepairing import of " path " in batches of 1000")
  (swap! conn update-max-tx-from-file path)
  (print "Importing batch: ")
  (time
   (doseq  [datoms (->> (line-seq (io/reader path))
                        (map read-string)
                        (partition 1000 1000 nil))]
     (print ".")
     (d/transact conn (vec datoms)))))

(defn upsert-test
  "Transact values in vector-of-values vov and print result for comparison."
  [conn entity attribute vov]
  (println "Starting Series on" entity " " attribute)
  (doseq [value vov]
    (print "transacting" value)
    (d/transact conn [{:db/id entity attribute value}])
    (println "-->" (attribute (d/pull @conn '[*] entity))))
  (println))

(defn upsert-test-auto
  "Test for two different upsert errors, print status and return true if we
  encounter one or both of the errors"
  [conn entity attribute]
  (println "Testing entity" entity "attribute" attribute)
  (let [transact-name (fn [v] (d/transact conn [{:db/id entity attribute v}]))
        pull-name     (fn [] (attribute (d/pull @conn '[*] entity)))
        _             (transact-name "Markus")
        _             (transact-name "Markus1")
        _             (transact-name "Markus")
        error1        (not= "Markus" (pull-name))
        _             (transact-name "Markus1")
        _             (transact-name "Markus2")
        _             (transact-name "Markus11")
        error2        (not= "Markus11" (pull-name))]
    (println "Errors:" error1 " " error2)
    (or error1 error2)))

(def mem-config {:store {:backend :mem
                         :id "temp"}
                 :schema-flexibility :write
                 :keep-history? true})

(defn run-test
  "runs a series of upsert-tests against a imported db"
  [conn eid file]
  (d/delete-database mem-config)
  (d/create-database mem-config)
  (def conn (d/connect mem-config))
  (import-db conn file)
  (upsert-test conn eid :name 
               ["Markus" "Markus1" "Markus" "Markus1" "Markus2" "Markus11"
                "Markus12" "Markus3"]))

(defn big-schema
  "Create one line of schema for :name and x lines of bloat schema for size"
  [x]
  (into [{:db/cardinality :db.cardinality/one
          :db/ident       :name
          :db/index       true
          :db/valueType   :db.type/string}]
        (for [i (range x)]
          {:db/cardinality :db.cardinality/one
           :db/ident       (keyword (str "attribute" i))
           :db/index       true
           :db/valueType   :db.type/string})))

(defn auto-test
  "Take the size of bloat of schema x, creates an in memory db of this size
  and check whether it shows upsert-errors.  Return true if there are Errors.
  Export vulnerable db."
  [x]
  (when (d/database-exists? mem-config)
    (d/delete-database mem-config))
  (let [_      (d/create-database mem-config)
        conn   (d/connect mem-config)
        schema (big-schema x)
        _      (d/transact conn schema)
        _      (d/transact conn
                 [{:name       "Markus"}])
        max-eid (:max-eid @conn)
        result  (upsert-test-auto conn max-eid :name)]
    (when result
      (export-db @conn "db-error"))
    (d/release conn)
    (d/delete-database mem-config)
    result))


(defn get-first-vulnerable-db-size [start]
  (when-not (auto-test start)
    (recur (inc start))))


(comment 

  ;; Build bigger and bigger database until we find one that is vulnerable.
  ;; On my sytem the first vulnerable size is 148 so we start searching at 140
  (get-first-vulnerable-db-size 140)
  ;; when a vulnerable db is found it is exportet to file "db-error"
  ;; user (run-test conn "db-error") on this file
  ;; or use the (upsert-test conn eid :name vector-of-values) below if the
  ;; db is already loaded.
  
  ;; on my machine the minimal vulnarable sizes are
  ;; one error on auto-test size 147 false true
  ;; two errors on auto-test size 148 true true

  ;; create a db of bloat-size 148 and check if vulnerable
  (auto-test 148)

  ;; run the detailed transactions against the file created by auto-test

  (d/create-database mem-config)
  (def conn
    (d/connect mem-config))
  (import-db conn "db-error")
  
  ;; run the sequence on in memory db
  (upsert-test conn 148 :name 
               ["Markus" "Markus1" "Markus" "Markus1" "Markus2" "Markus11"
                "Markus12" "Markus3"])

  (d/delete-database mem-config)
  
  (d/release conn)

  )
