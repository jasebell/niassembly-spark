(ns mlas.core
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as s-de]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            )
  (:gen-class))

(def members-path "/Users/Jason/work/data/niassembly/members.json")
(def questions-path "/Users/Jason/work/data/niassembly/questions/")
(def api-questions-by-member "http://data.niassembly.gov.uk/questions.asmx/GetQuestionsByMember_JSON?personId=")

(defn load-members [sc filepath] 
  (->> (spark/whole-text-files sc filepath)
       (spark/flat-map (s-de/key-value-fn (fn [key value] 
                                            (-> (json/read-str value 
                                                               :key-fn (fn [key] 
                                                                         (-> key
                                                                             str/lower-case
                                                                             keyword)))
                                                (get-in [:allmemberslist :member])))))
       (spark/map-to-pair (fn [rec] (spark/tuple (:personid rec) rec)))))

(defn save-question-data [pair-rdd] 
  (->> pair-rdd 
       (spark/map (s-de/key-value-fn (fn [key value] 
                                       (spit (str questions-path key ".json") 
                                             (slurp (str api-questions-by-member key))))))))

(defn load-questions [sc questions-path] 
  (->> (spark/whole-text-files sc questions-path)
       (spark/flat-map (s-de/key-value-fn (fn [key value] 
                                            (-> (json/read-str value
                                                 :key-fn (fn [key] 
                                                           (-> key 
                                                               str/lower-case 
                                                               keyword)))
                                                (get-in [:questionslist :question])))))
       (spark/map-to-pair (fn [rec] (spark/tuple (:tablerpersonid rec) rec)))
       (spark/group-by-key)))

(defn join-members-questions [members-rdd questions-rdd]
  (spark/join members-rdd questions-rdd))

(defn department-frequencies-rdd [members-questions-rdd] 
  (->> members-questions-rdd
       (spark/map-to-pair 
        (s-de/key-val-val-fn (fn [key member questions]
                               (let [freqmap (map (fn [question] (:departmentname question)) questions)]
                                 (spark/tuple key (frequencies freqmap))))))))

(comment
  (def c (-> (conf/spark-conf)
             (conf/master "local[3]")
             (conf/app-name "niassemblydata-sparkjob")))
  (def sc (spark/spark-context c))

)
 
