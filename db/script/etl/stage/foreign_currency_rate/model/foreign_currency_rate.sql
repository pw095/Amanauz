DROP TABLE foreign_currency_rate;
CREATE TABLE foreign_currency_rate
  (
    tech$load_id   INTEGER NOT NULL,
    tech$load_dttm TEXT    NOT NULL,
    trade_date     TEXT,
    id             TEXT,
    nominal        INTEGER,
    value          REAL
  );
