(ns lazyskulptor.es.entry
  (:require
    [lazyskulptor.es.spec]
    [lazyskulptor.es.core :as core]
    [lazyskulptor.es.impl]
    [clojure.spec.alpha :as s]))
(import java.time.Instant)


;; utils
(defn- seek-oldest [seqs before?]
  (loop [oldest (first seqs) sub (next seqs) index 0 cursor 1]
    (cond
      (nil? sub)
      (list (first oldest)
            (when seqs
              (filterv some? (assoc seqs index (next oldest)))))
      (and oldest (before? (second sub) (first oldest)))
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
                       (list oldest)))))
        seqs (mapv f ids)]
    (select seqs)))

(defn- as-coll [x]
  (if (coll? x) x [x]))

;; interface
(defn save-event [events] (core/save-event events))

(defn by-entity-ids
  ([entity-id-list] (merge-stream core/by-entity-id (as-coll entity-id-list)))
  ([entity-id-list event-type time] (merge-stream #(core/by-entity-id % event-type time) (as-coll entity-id-list))))

(defn by-event-types
  ([type-name-list] (merge-stream core/by-event-type (as-coll type-name-list)))
  ([type-name-list time] (merge-stream #(core/by-event-type % time) (as-coll type-name-list))))

(defn list-tb [] (core/list-tb :default))
(defn set-env [dynamo-option table-name] (core/set-env dynamo-option table-name))
