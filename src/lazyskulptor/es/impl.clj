(ns lazyskulptor.es.impl
  (:require [lazyskulptor.es.core :as core]
            [taoensso.faraday :as far]
            [clojure.spec.alpha :as s]))
(import java.time.Instant)

;; state
(def ^:private ^:dynamic *client-opts* (atom {}))

(def ^:private ^:dynamic *tbname* (atom ""))

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

;; core implementation
(defmethod ^:private core/set-env :default [config tbname]
  (swap! *client-opts* (fn [_] config))
  (swap! *tbname* (fn [_] tbname))
  (if-let [desc (far/describe-table @*client-opts* @*tbname*)]
    desc
    (create-tb @*client-opts* @*tbname*)))

(defn- query [prim-key-conds opts]
  (far/query @*client-opts* @*tbname*
             prim-key-conds
             (conj {:limit 100} opts)))

(defmethod ^:private core/save-event :default
  [events]
  {:pre [(s/valid? (s/or :single :lazyskulptor.es/event :coll (s/coll-of :lazyskulptor.es/event)) events)]}
  (let [events-coll (if (vector? events) events [events])]
    (far/transact-write-items
      @*client-opts*
      {:items (map #(vector :put {:table-name @*tbname* :item %})
                   (map init-event events-coll))})))

(defn- by-id [id event-type time last-id]
  {:pre [(s/and (s/valid? :lazyskulptor.es/entity-id id)
                (s/valid? (s/or :nil nil? :not-nil :lazyskulptor.es/event-type) event-type)
                (s/valid? :lazyskulptor.es/time time))]}
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
  {:pre [(s/and (s/valid? :lazyskulptor.es/event-type type)
                (s/valid? :lazyskulptor.es/time time))]}
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

(defmethod ^:private core/by-event-type :single-args [event-type & _args]
  (core/by-event-type event-type nil))

(defmethod ^:private core/by-event-type :multi-args [event-type & args]
  (by-type event-type (min-time (ensure-instant (first args))) nil))

(defmethod ^:private core/by-entity-id :single-args [id & _args]
  (core/by-entity-id id nil nil))

(defmethod ^:private core/by-entity-id :multi-args [id & args]
  (by-id id
         (first args)
         (min-time (ensure-instant (second args)))
         nil))

(defmethod ^:private core/list-tb :default [_]
  (far/list-tables @*client-opts*))
