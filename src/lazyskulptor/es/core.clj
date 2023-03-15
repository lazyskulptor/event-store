(ns lazyskulptor.es.core)

;; util functions
(defn- multi-args? [& args]
  (if (> (count args) 1) :multi-args :single-args))
(defn- as-default [& _] :default)

(defmulti set-env as-default)


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
          as-default)


(defmulti by-entity-id multi-args?)
(defmulti by-event-type
          "Return lazy seq matching event-type after time"
          multi-args?)

(defmulti list-tb identity)