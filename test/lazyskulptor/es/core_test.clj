(ns lazyskulptor.es.core-test
  (:require
   [lazyskulptor.es.core :refer [*client-opts* *tbname* save-event by-entity-id by-entity-ids by-event-type by-event-types]]
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


(deftest by-entity-id-test
  (testing "Save event with uuid"
    (let [uuid (-> (random) .toString)]
      (save-event {:entity-id uuid
                   :event-type "test-create-club",
                   :value {}})
      (is (= uuid
             (:entity-id (first (by-entity-id uuid nil nil))))))))

(deftest by-entity-ids-test
  (testing "Save event with uuid"
    (let [uuid1 (-> (random) .toString)
          uuid2 (-> (random) .toString)]

      (save-event {:entity-id uuid1 :event-type "create", :value {}})
      (save-event {:entity-id uuid2 :event-type "create", :value {}})
      (doseq [_ (range 10)]
        (save-event {:entity-id uuid1 :event-type "update", :value {}}))
      (doseq [_ (range 10)]
        (save-event {:entity-id uuid2 :event-type "update", :value {}}))
      (let [result (by-entity-ids [uuid1 uuid2])]
        (is (= uuid1 (:entity-id (first result))))
        (is (= 22 (count result)))))))

(deftest by-event-type-test
  (testing "Save event with uuid"
    (let [uuid (-> (random) .toString)
          event-type "test-create-club"]
      (save-event {:entity-id uuid
                   :event-type event-type,
                   :value {}})
      (is (= event-type
             (:event-type (first (by-event-type event-type nil))))))))
