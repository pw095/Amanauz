DROP TABLE index_security_weight;
CREATE TABLE index_security_weight
  (
    index_name   TEXT    NOT NULL,
    trade_date   TEXT    NOT NULL,
    secid        TEXT    NOT NULL,
    weight       REAL    NOT NULL,
    tech$load_id INTEGER NOT NULL
  );
