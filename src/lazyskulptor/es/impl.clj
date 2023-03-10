(ns lazyskulptor.es.impl
  (:require [lazyskulptor.es.core :as core]
            [taoensso.faraday :as far]
            [clojure.spec.alpha :as s]))
(import java.time.Instant)

;; state
(def ^:dynamic *client-opts* (atom {}))

(def ^:dynamic *tbname* (atom ""))

;; util fn
(defn- min-time
  ([] (min-time nil))
  ([t] (if (some? t) t (Instant/MIN))))

(defn- time-ordered []
  (com.github.f4b6a3.uuid.UuidCreator/getTimeOrdered))

(defn- init-event [event]
  (assoc event
    :uuid (.toString (time-ordered))
    :time (.toString (Instant/now))))

(defn time-spec [time]
  `(s/valid? (s/or :nil nil?
                   :date #(= (type %) java.util.Date)
                   :instant #(= (type %) Instant))
             ~time))

(defn ensure-instant [time]
  (if (and (not= nil time) (instance? java.util.Date time))
    (.toInstant time)
    time))

(defn create-tb
  [client-opts tbname]
  (far/create-table
    client-opts tbname
    [:entity-id :s]                                         ; Primary key named "id", (:n => number type)
    {:range-keydef [:uuid :s]
     :throughput   {:read 1 :write 1}                       ; Read & write capacity (units/sec)
     :gsindexes    [{:name         :event-type
                     :hash-keydef  [:event-type :s]
                     :range-keydef [:uuid :s]
                     :projection   [:time :entity-id]
                     :throughput   {:read 1 :write 1}}]
     :block?       true}))

(defn exist-tb? [tbname]
  (let [tbs (core/list-tb :default)]
    (some #{tbname} tbs) ))

;; core implementation
(defmethod core/set-env :default [config tbname]
  (swap! *client-opts* (fn [_] config))
  (swap! *tbname* (fn [_] tbname))
  (when (not (exist-tb? @*tbname*))
    (create-tb @*client-opts* @*tbname*)))

(defn- query [prim-key-conds opts]
  (far/query @*client-opts* @*tbname*
             prim-key-conds
             (conj {:limit 100} opts)))

(defmethod core/save-event :default
  [events]
  {:pre [(s/valid? (s/or :single :lazyskulptor.es.core/event :coll (s/coll-of :lazyskulptor.es.core/event)) events)]}
  (let [events-coll (if (vector? events) events [events])]
    (far/transact-write-items
      @*client-opts*
      {:items (map #(vector :put {:table-name @*tbname* :item %})
                   (map init-event events-coll))})))

(defn- by-id [id event-type time last-id]
  {:pre [(s/and (s/valid? some? id)
                (time-spec time))]}
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

(defmethod core/by-event-type :single-args [event-type & _args]
  (core/by-event-type event-type nil))

(defmethod core/by-event-type :multi-args [event-type & args]
  (by-type event-type (min-time (ensure-instant (first args))) nil))

(defmethod core/by-entity-id :single-args [id & _args]
  (core/by-entity-id id nil nil))

(defmethod core/by-entity-id :multi-args [id & args]
  (by-id id
         (first args)
         (min-time (ensure-instant (second args)))
         nil))

(defmethod core/list-tb :default [_]
  (far/list-tables @*client-opts*))