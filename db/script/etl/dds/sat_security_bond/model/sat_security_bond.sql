DROP TABLE IF EXISTS sat_security_bond;
CREATE TABLE sat_security_bond
  (
    tech$load_id       INTEGER NOT NULL,
    tech$hash_key      TEXT    NOT NULL,
    tech$effective_dt  TEXT    NOT NULL,
    tech$expiration_dt TEXT    NOT NULL,
    tech$record_source TEXT    NOT NULL,
    tech$hash_value    TEXT    NOT NULL,
    coupon_value       REAL    NOT NULL,
    coupon_percent     REAL,
    coupon_period      INTEGER NOT NULL,
    mature_date        TEXT    NOT NULL,
    buy_back_date      TEXT,
    buy_back_price     REAL,
    offer_date         TEXT    NOT NULL,
    PRIMARY KEY(tech$hash_key, tech$effective_dt),
    FOREIGN KEY (tech$hash_key) REFERENCES hub_security(tech$hash_key) ON DELETE CASCADE
  )
WITHOUT ROWID;
