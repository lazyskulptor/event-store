(ns lazyskulptor.es.core-test
  (:require
   [lazyskulptor.es.core :refer [*client-opts* *tbname* save-event by-entity-id by-event-type]]
   [taoensso.faraday :as far])
  (:require
   [clojure.test :refer :all]))

(defn create-tb
  ([] (create-tb @*client-opts* @*tbname*))
  ([client-opts tbname]
   (far/create-table
    client-opts  tbname
    [:entity-id :s]  ; Primary key named "id", (:n => number type)
    {:range-keydef [:uuid :s]
     :throughput {:read 1 :write 1} ; Read & write capacity (units/sec)
     :gsindexes [{:name :event-type
                  :hash-keydef [:event-type :s]
                  :range-keydef [:uuid :s]
                  :projection [:time :entity-id]
                  :throughput {:read 1 :write 1}}]
     :block? true}))) ; Block thread during table creation

(defn del-tb [opts tb] (far/delete-table opts tb))

(defn list-tb
  ([] (list-tb @*client-opts*))
  ([opts] (far/list-tables opts)))

(defn scan
  [] (far/scan @*client-opts* @*tbname*))

(defn random []
  (random-uuid))

(use-fixtures
  :once
  (fn [f]
    (swap! *client-opts* (fn [_] {:access-key "fakeMyKeyId"
                                  :secret-key "fakeSecretAccessKey"
                                  :endpoint "http://localhost:8000"}))
    (swap! *tbname* (fn [_] "event-table"))
    (del-tb @*client-opts*  @*tbname*)
    (create-tb @*client-opts*  @*tbname*)
    (f)
    ))


(deftest test-save-event
  (testing "Save event with uuid"
    (let [uuid (-> (random) .toString)]
      (save-event {:entity-id uuid
                   :event-type "test-create-club",
                   :value {}})
      (is (= uuid
             (:entity-id (first (by-entity-id uuid nil nil))))))))

(deftest test-save-event
  (testing "Save event with uuid"
    (let [uuid (-> (random) .toString)
          event-type "test-create-club"]
      (save-event {:entity-id uuid
                   :event-type event-type,
                   :value {}})
      (is (= event-type
             (:event-type (first (by-entity-id uuid nil nil))))))))
