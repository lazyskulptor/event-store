(ns lazyskulptor.es.entry
  (:require
    [lazyskulptor.es.spec]
    [lazyskulptor.es.core :as core]
    [lazyskulptor.es.impl :as impl])
  (:gen-class
   :name lazyskulptor.es.EventStore
   :init init
   :state state
   :constructors {[Object String] []}
   :factory emit
   :main false))

;; interface
(defn boot [client-opts tb-name]
  (core/functions
   (impl/query client-opts tb-name)
   (impl/save-event-fn client-opts tb-name)
   (impl/list-tb client-opts)))

;; java interface

(defn -init [dynamo-option table-name]
  [[] (ref (boot dynamo-option table-name))])

(defn -save-event [this event]
  (let [state (.state this)] ((@state core/save-event) event)))

(defn -by-entity-ids 
  ([this entity-id-list]
   (let [state (.state this)] ((@state core/by-id) entity-id-list)))
  ([this entity-id-list event-type time]
   (let [state (.state this)]
     ((@state core/by-id) entity-id-list event-type time))))
