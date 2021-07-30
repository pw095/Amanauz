UPDATE security_daily_info_bonds
   SET lot_value = settle_date,
       settle_date = offer_date,
       offer_date = null
 WHERE tech$load_id < 55;

CREATE TABLE security_daily_info_bonds_copy AS
SELECT *
  FROM security_daily_info_bonds
WHERE tech$load_id = 55;

UPDATE security_daily_info_bonds
   SET offer_date = (SELECT offer_date
                       FROM security_daily_info_bonds_copy t
                      WHERE t.security_id = security_daily_info_bonds.security_id
                        AND t.board_id = security_daily_info_bonds.board_id)
 WHERE tech$load_id < 55;

DROP TABLE security_daily_info_bonds_copy;
