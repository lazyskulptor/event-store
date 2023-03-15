(ns lazyskulptor.es.spec
  (:require
    [clojure.spec.gen.alpha :as gen]
    [clojure.spec.alpha :as s]))

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
