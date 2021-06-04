DROP TABLE IF EXISTS security_emitent_map;
CREATE TABLE security_emitent_map
  (
    tech$load_id          INTEGER NOT NULL,
    tech$load_dttm        TEXT    NOT NULL,
    id                    INTEGER,
    security_id           TEXT,
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
    market_price_board_id TEXT
  );
