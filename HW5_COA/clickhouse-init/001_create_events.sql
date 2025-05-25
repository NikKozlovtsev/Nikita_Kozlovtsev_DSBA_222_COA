CREATE TABLE IF NOT EXISTS default.events (
  user_id    String,
  entity_id  String,
  event      String,
  timestamp  DateTime
) ENGINE = MergeTree()
ORDER BY (entity_id, event, timestamp);