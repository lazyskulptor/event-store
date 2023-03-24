(ns lazyskulptor.es.impl
  (:require [taoensso.faraday :as far]))
(import java.time.Instant)

;; util fn
(defn- time-ordered []
  (com.github.f4b6a3.uuid.UuidCreator/getTimeOrdered))

(defn- init-event [event]
  (assoc event
    :uuid (.toString (time-ordered))
    :time (.toString (Instant/now))))

(defn- create-tb
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

(defn ensure-tb [client-opts tb-name]
  (if-let [desc (far/describe-table client-opts tb-name)]
    desc (create-tb client-opts tb-name)))

(defn query [client-opts tb-name]
  (fn [prim-key-conds opts]
    (far/query client-opts tb-name
               prim-key-conds
               (conj {:limit 100} opts))))

(defn save-event-fn [client-opts tb-name]
  (fn [events-coll]
    (far/transact-write-items
     client-opts
     {:items (map #(vector :put {:table-name tb-name :item %})
                  (map init-event events-coll))})))

(defn list-tb [client-opts]
  #(far/list-tables client-opts))

