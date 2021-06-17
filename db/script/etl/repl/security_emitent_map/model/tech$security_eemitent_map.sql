DROP TABLE IF EXISTS tech$security_emitent_map;
CREATE TABLE tech$security_emitent_map
  (
    tech$load_id          INTEGER NOT NULL,
    tech$effective_dt     TEXT    NOT NULL,
    tech$expiration_dt    TEXT    NOT NULL,
    tech$hash_value       TEXT    NOT NULL,
    id                    INTEGER NOT NULL,
    security_id           TEXT    NOT NULL,
    short_name            TEXT    NOT NULL,
    reg_number            TEXT    NOT NULL,
    security_name         TEXT    NOT NULL,
    isin                  TEXT    NOT NULL,
    security_is_traded    INTEGER NOT NULL,
    emitent_id            INTEGER NOT NULL,
    emitent_title         TEXT    NOT NULL,
    emitent_inn           TEXT    NOT NULL,
    emitent_okpo          TEXT    NOT NULL,
    emitent_gos_reg       TEXT    NOT NULL,
    security_type         TEXT    NOT NULL,
    security_group        TEXT    NOT NULL,
    primary_board_id      TEXT    NOT NULL,
    market_price_board_id TEXT    NOT NULL,
    PRIMARY KEY(security_id, tech$effective_dt)
  )
WITHOUT ROWID;
