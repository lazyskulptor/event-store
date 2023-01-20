(ns lazyskulptor.es.core
  (:require
   [taoensso.faraday :as far]
   [clojure.spec.alpha :as s]))

(def ^:dynamic *client-opts* (atom {}))

(def ^:dynamic *tbname* (atom ""))

(defn- min-time
  ([] (min-time nil))
  ([t] (if (some? t) t (java.time.Instant/MIN))))

(defn- event? [param]
  (and (contains? param :event-type) ;; not nil
       (contains? param :entity-id) ;; uuid
       (contains? param :value))) ;; could be nil

(defn- time-ordered []
  (com.github.f4b6a3.uuid.UuidCreator/getTimeOrdered))

(defn- init-event [event]
  (assoc event
         :uuid (.toString (time-ordered))
         :time (.toString (java.time.Instant/now))))

(defn save-event [events]
  {:pre [(s/valid? (s/or :single event? :coll (s/coll-of event?)) events)]}
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
  '(s/valid? (s/or :nil nil?
                  :date #(= (type %) java.util.Date)
                  :instant #(= (type %) java.time.Instant))
            ~time))

(defn ensure-instant [time]
  (if (and (not= nil time) (instance? time java.util.Date))
    (.toInstant time)
    time))

(defn by-entity-id [id event-type time]
  {:pre [(time-spec time)]}
   (by-id id
          event-type
          (min-time (ensure-instant time))
          nil))

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

(defn by-event-type [event-type time]
  {:pre [(time-spec time)]}
  (by-type event-type (min-time (ensure-instant time)) nil))
