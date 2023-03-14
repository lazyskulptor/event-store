(ns lazyskulptor.es.core
  (:require
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.alpha :as s]))
(import java.time.Instant)

;; specs
(s/def :lazyskulptor.es.core/uuid-string
  (s/with-gen #(uuid? (java.util.UUID/fromString %))
    #(gen/fmap (fn [_] (.toString (random-uuid))) (gen/string))))
(s/def :lazyskulptor.es.core/entity-id :lazyskulptor.es.core/uuid-string)
(s/def :lazyskulptor.es./event-type string?)
(s/def :lazyskulptor.es.core/value (s/or :map map? :nil nil?))

(s/def :lazyskulptor.es.core/event
  (s/keys :req-un [:lazyskulptor.es.core/entity-id
                   :lazyskulptor.es.core/event-type
                   :lazyskulptor.es.core/value]))

;; util functions
(defmulti set-env identity)

(defn- multi-args? [args]
  (if (> (count args) 1) :multi-args :single-args))

(defn- seek-oldest [seqs before?]
  (loop [oldest (first seqs) sub (next seqs) index 0 cursor 1]
    (cond
      (nil? sub)
      (list (first oldest)
            (when seqs
              (filterv some? (assoc seqs index (next oldest)))))
      (and oldest (before? (ffirst sub) (first oldest)))
      (recur (first sub) (next sub) cursor (inc cursor))
      :else (recur oldest (next sub) index (inc cursor)))))

(defn- merge [f ids]
  {:pre [(s/valid? coll? ids)]}
  (let [before? (fn [d1 d2]
                  (.isBefore (Instant/parse (:time d1))
                             (Instant/parse (:time d2))))
        select (fn worker [seqs]
                 (lazy-seq
                   (let [[oldest next-seqs] (seek-oldest seqs before?)]
                     (if (first next-seqs)
                       (cons oldest (worker next-seqs))
                       (list oldest)))))
        seqs (mapv f ids)]
    (select seqs)))


;; core functions
(defmulti save-event
  "Persist coll of events
  
  ```clojure
  (save-event {:entity-id \"uuid\"
               :event-type \"test-create-club\",
               :value {}})
  ```
  - events coll of map consisting of three keys :entity-id, :event-type, : value
  "
  identity)


(defmulti by-entity-id multi-args?)
(defmulti by-event-type
          "Return lazy seq matching event-type after time"
          multi-args?)

(defn by-entity-ids
  ([ids] (merge by-entity-id ids))
  ([ids event-type time] (merge #(by-entity-id % event-type time) ids)))

(defn by-event-types
  ([ids] (merge by-event-type ids))
  ([ids time] (merge #(by-event-type % time) ids)))
