DROP TABLE IF EXISTS security_emitent_map;
CREATE TABLE security_emitent_map
  (
    tech$load_id          INTEGER NOT NULL,
    tech$effective_dt     TEXT    NOT NULL,
    tech$expiration_dt    TEXT    NOT NULL,
    tech$last_seen_dt     TEXT    NOT NULL,
    tech$hash_value       TEXT    NOT NULL,
    id                    INTEGER,
    security_id           TEXT    NOT NULL,
    short_name            TEXT,
    reg_number            TEXT,
    security_name         TEXT,
    isin                  TEXT,
    security_is_traded    INTEGER,
    emitent_id            INTEGER,
    emitent_title         TEXT,
    emitent_inn           TEXT,
    emitent_okpo          TEXT,
    emitent_gos_reg       TEXT,
    security_type         TEXT,
    security_group        TEXT,
    primary_board_id      TEXT,
    market_price_board_id TEXT,
    PRIMARY KEY(security_id, tech$effective_dt)
  )
WITHOUT ROWID;
