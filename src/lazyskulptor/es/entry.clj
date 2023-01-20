(ns lazyskulptor.es.entry
  (:require
   [lazyskulptor.es.core :refer [*client-opts* *tbname*]]))

(defn set-clients [config]
  (swap! *client-opts* (fn [_] config)))

(defn set-tbname [name]
  (swap! *tbname* (fn [_] name)))
