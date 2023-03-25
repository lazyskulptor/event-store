(ns lazyskulptor.es.core
  (:require [clojure.spec.alpha :as s]))
(import java.time.Instant)

;; util functions
(defn- min-time
  ([] (min-time nil))
  ([t] (if (some? t) t (Instant/MIN))))

(defn- ensure-instant [time]
  (cond (and (not= nil time) (instance? java.util.Date time)) (.toInstant time)
        (not= nil time) time
        :else (min-time time)))

(def save-event ::save-event)
(def by-id ::by-id)
(def by-type ::by-type)
(def list-tb ::list-tb)


;; core functions
(defn- by-id-fn [query-fn]
  (fn iter [id event-type time last-id]
    {:pre [(s/and (s/valid? :lazyskulptor.es/entity-id id)
                  (s/valid? (s/or :nil nil? :not-nil :lazyskulptor.es/event-type) event-type)
                  (s/valid? :lazyskulptor.es/time time))]}
    (lazy-seq
     (let [events (query-fn
                   (conj {:entity-id [:eq id]}
                         (when (some? last-id)
                           {:uuid [:gt last-id]}))
                   {:query-filter (conj
                                   {:time [:gt (.toString (min-time time))]}
                                   (when event-type {:event-type [:eq event-type]}))})]
       (if (> (count events) 0)
         (concat events (iter id event-type time (:uuid (peek events))))
         events)))))

(defn- by-type-fn [query-fn]
  (fn iter [type time last-id]
    {:pre [(s/and (s/valid? :lazyskulptor.es/event-type type)
                  (s/valid? :lazyskulptor.es/time time))]}
    (lazy-seq
     (let [events (query-fn
                   (conj {:event-type [:eq type]}
                         (when (some? last-id)
                           {:uuid [:gt last-id]}))
                   {:query-filter {:time [:gt (.toString time)]}
                    :index :event-type})]
       (if (> (count events) 0)
         (concat events (iter type time (:uuid (peek events))))
         events)))))

(defn- save-fn [save-fn]
  (fn [events]
    {:pre [(s/valid?
            (s/or :single :lazyskulptor.es/event :coll
                  (s/coll-of :lazyskulptor.es/event)) events)]}
    (let [events-coll (if (vector? events) events [events])]
      (save-fn events-coll))))

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

(defn- merge-stream [f ids]
  {:pre [(s/valid? coll? ids)]}
  (let [before? (fn [d1 d2]
                  (.isBefore (Instant/parse (:time d1))
                             (Instant/parse (:time d2))))
        select (fn worker [seqs]
                 (lazy-seq
                   (let [[oldest next-seqs] (seek-oldest seqs before?)]
                     (if (first next-seqs)
                       (cons oldest (worker next-seqs))
                       (if oldest (list oldest) (list))))))
        seqs (mapv f ids)]
    (select seqs)))

(defn- as-coll [x]
  (if (coll? x) x [x]))


(defn functions [query-fn save-event-fn list-fn]
  (let [save (save-fn save-event-fn)
        by-entity-id (fn internal-fn
                       ([id] (internal-fn id nil nil))
                       ([id event-type time] ((by-id-fn query-fn) id event-type (ensure-instant time) nil)))
        by-event-type (fn internal-fn
                        ([type-name] (internal-fn type-name nil))
                        ([type-name time] ((by-type-fn query-fn) type-name (ensure-instant time) nil)))
        by-entity-ids (fn
                        ([id-list] (merge-stream by-entity-id (as-coll id-list)))
                        ([id-list event-type time] (merge-stream #(by-entity-id % event-type time) (as-coll id-list))))
        by-event-types (fn
                        ([name-list] (merge-stream by-event-type (as-coll name-list)))
                        ([name-list time] (merge-stream #(by-event-type % time) (as-coll name-list))))]

    #(cond (= % save-event) save
           (= % by-id) by-entity-ids
           (= % by-type) by-event-types
           (= % list-tb) list-fn
           :else (throw (IllegalArgumentException. "No Defined functions")))))
