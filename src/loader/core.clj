(ns loader.core
  (:gen-class)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv])

  (:import (java.io FileInputStream PipedOutputStream PipedInputStream)
           java.util.zip.GZIPInputStream
           org.postgresql.copy.CopyManager))


(def db-uri "jdbc:postgresql://127.0.0.1/project?user=project&password=project")

(def path-src "/Users/ivan/Downloads/out.gzip")


(defn get-lines
  []
  (-> path-src
      FileInputStream.
      GZIPInputStream.
      io/reader
      line-seq))


(def enumerate (partial map-indexed vector))


(defn proces-lines
  [lines]
  (let [conn (jdbc/get-connection
              {:connection-uri db-uri})

        copy (CopyManager. conn)

        out-stream (PipedOutputStream.)
        in-stream (PipedInputStream. out-stream)]

    (future

      (with-open [writer (io/writer out-stream)]
        (csv/write-csv
         writer
         (for [[i line] (enumerate lines)]
           [i line])))

      (.close out-stream))

    (deref
     (future
       (.copyIn copy "COPY fhir FROM STDIN (FORMAT csv, HEADER false)" in-stream)))))


(def step 10000)

(def by-chunks (partial partition step step []))


(defn multi-loader
  []
  (let [lines (get-lines)
        chunks (by-chunks lines)]
    (doall (pmap proces-lines chunks))
    nil))
