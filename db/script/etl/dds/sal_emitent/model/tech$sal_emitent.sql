DROP TABLE IF EXISTS tech$sal_emitent;
CREATE TABLE tech$sal_emitent
  (
    tech$load_id               INTEGER NOT NULL,
    tech$hash_key              TEXT    NOT NULL,
    tech$record_source         TEXT    NOT NULL,
    tech$load_dt               TEXT    NOT NULL,
    tech$last_seen_dt          TEXT    NOT NULL,
    emitent_master_hash_key    TEXT    NOT NULL,
    emitent_duplicate_hash_key TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key),
    UNIQUE(emitent_master_hash_key, emitent_duplicate_hash_key)
  )
WITHOUT ROWID;
