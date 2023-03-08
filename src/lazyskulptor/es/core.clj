(ns lazyskulptor.es.core
  (:require
   [taoensso.faraday :as far]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.alpha :as s]))
(import java.time.Instant)

(def ^:dynamic *client-opts* (atom {}))

(def ^:dynamic *tbname* (atom ""))

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

(defn- min-time
  ([] (min-time nil))
  ([t] (if (some? t) t (Instant/MIN))))

(defn- time-ordered []
  (com.github.f4b6a3.uuid.UuidCreator/getTimeOrdered))

(defn- init-event [event]
  (assoc event
         :uuid (.toString (time-ordered))
         :time (.toString (Instant/now))))

(defn save-event
  "Persist coll of events
  
  ```clojure
  (save-event {:entity-id \"uuid\"
               :event-type \"test-create-club\",
               :value {}})
  ```
  - events coll of map consisting of three keys :entity-id, :event-type, : value
  "
  [events]
  {:pre [(s/valid? (s/or :single :lazyskulptor.es.core/event :coll (s/coll-of :lazyskulptor.es.core/event)) events)]}
  (let [events-coll (if (vector? events) events [events])]
    (far/transact-write-items
     @*client-opts*
     {:items (map #(vector :put {:table-name @*tbname* :item %})
                  (map init-event events-coll))})))

(defn- query [prim-key-conds opts]
  (far/query @*client-opts* @*tbname*
             prim-key-conds
             (conj {:limit 100} opts)))

(defn- by-id [id event-type time last-id]
  {:pre [(s/valid? some? id)]}
  (lazy-seq
   (let [events (query
                 (conj {:entity-id [:eq id]}
                       (when (some? last-id)
                         {:uuid [:gt last-id]}))
                 {:query-filter (conj
                                 {:time [:gt (.toString (min-time time))]}
                                 (when event-type {:event-type [:eq event-type]}))})]
     (if (> (count events) 0)
       (concat events (by-id id event-type time (:uuid (peek events))))
       events))))

(defn time-spec [time]
  `(s/valid? (s/or :nil nil?
                   :date #(= (type %) java.util.Date)
                   :instant #(= (type %) Instant))
             ~time))

(defn ensure-instant [time]
  (if (and (not= nil time) (instance? time java.util.Date))
    (.toInstant time)
    time))

(defn by-entity-id
  "Return lazy seq matching entity-id and event-type after time"
  ([id] (by-entity-id id nil nil))
  ([id event-type time]
   {:pre [(time-spec time)]}
   (by-id id
          event-type
          (min-time (ensure-instant time))
          nil)))


(defn- by-type [type time last-id]
  (lazy-seq
   (let [events (query
                 (conj {:event-type [:eq type]}
                       (when (some? last-id)
                         {:uuid [:gt last-id]}))
                 {:query-filter {:time [:gt (.toString time)]}
                  :index :event-type})]
     (if (> (count events) 0)
       (concat events (by-type type time (:uuid (peek events))))
       events))))

(defn by-event-type
  "Return lazy seq matching event-type after time"
  ([event-type] (by-event-type event-type nil))
  ([event-type time]
   {:pre [(time-spec time)]}
   (by-type event-type (min-time (ensure-instant time)) nil)))

;;
(defn- merged [f ids]
  {:pre [(s/valid? coll? ids)]}
  (let [before? (fn [d1 d2] (> (.compareTo d1 d2) 0))
        seek-oldest (fn [seqs]
                      (loop [oldest (first seqs) sub (next seqs) index 0 cursor 1]
                        (cond
                          (nil? sub)
                            (list oldest index)
                          (before? (first oldest) (ffirst sub))
                            (recur (first sub) (next sub) cursor (inc cursor))
                          :else (recur oldest (next sub) index (inc cursor)))))
        worker (fn worker [sub-seqs]
                 (let [[oldest index] (seek-oldest sub-seqs)]
                   (cons oldest
                         (worker (assoc sub-seqs index (next (nth index sub-seqs)))))))
        seqs (map f ids)]
    (worker seqs)))

(defn by-entity-ids
  ([ids] (merged by-entity-id ids))
  ([ids event-type time] (merged #(by-entity-id % event-type time) ids)))

(defn by-entity-types
  ([ids] (merged by-event-type ids))
  ([ids time] (merged #(by-event-type % time) ids)))
