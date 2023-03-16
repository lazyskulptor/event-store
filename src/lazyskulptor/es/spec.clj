(ns lazyskulptor.es.spec
  (:require
    [clojure.spec.gen.alpha :as gen]
    [clojure.spec.alpha :as s]))

(s/def :lazyskulptor.es/uuid-string
  (s/with-gen #(uuid? (java.util.UUID/fromString %))
              #(gen/fmap (fn [_] (.toString (random-uuid))) (gen/string))))
(s/def :lazyskulptor.es/entity-id :lazyskulptor.es/uuid-string)
(s/def :lazyskulptor.es/event-type (s/or :string string? :keyword keyword?))
(s/def :lazyskulptor.es/value (s/or :map map? :nil nil?))
(s/def :lazyskulptor.es/time (s/or :nil nil?
                   :date #(= (type %) java.util.Date)
                   :instant #(= (type %) java.time.Instant)))

(s/def :lazyskulptor.es/event
  (s/keys :req-un [:lazyskulptor.es/entity-id
                   :lazyskulptor.es/event-type
                   :lazyskulptor.es/value]))
