(ns lazyskulptor.es.core-test
  (:require
    [lazyskulptor.es.entry :refer [set-env save-event by-entity-ids by-event-types]]
    [taoensso.faraday :as far])
  (:require
    [clojure.test :refer :all]))

(def test-opts {:access-key "fakeMyKeyId"
                :secret-key "fakeSecretAccessKey"
                :endpoint   (str "http://" (or (System/getenv "DYNAMO_HOST") "localhost") ":8000")})
(def test-tb "event-table")
(defn del-tb [opts tb] (far/delete-table opts tb))

(defn scan
  [] (far/scan test-opts test-tb))

(defn random []
  (random-uuid))

(use-fixtures
  :once
  (fn [f]
    (set-env test-opts test-tb)
    (println "DYNAMO OPT :: " test-opts)
    (println "TABLE NAME :: " test-tb)
    (f)
    (del-tb test-opts test-tb)))


(deftest by-entity-id-test
  (testing "Save event with uuid"
    (let [uuid (-> (random) .toString)]
      (save-event {:entity-id  uuid
                   :event-type "test-create-club",
                   :value      {}})
      (is (= uuid
             (:entity-id (first (by-entity-ids uuid))))))))

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
      (save-event {:entity-id  uuid
                   :event-type event-type,
                   :value      {}})
      (is (= event-type
             (:event-type (first (by-event-types event-type))))))))
