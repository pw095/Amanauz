CREATE TABLE security_daily_info_shares_copy AS
SELECT *
  FROM security_daily_info_shares
 WHERE tech$load_id = 12;

UPDATE security_daily_info_shares
   SET (face_unit, issue_size, isin, currency_id) = (SELECT
                                                            face_unit,
                                                            issue_size,
                                                            isin,
                                                            currency_id
                                                       FROM security_daily_info_shares_copy t
                                                      WHERE t.security_id = security_daily_info_shares.security_id
                                                        AND t.board_id = security_daily_info_shares.board_id)
 WHERE tech$load_id < 12;

DROP TABLE security_daily_info_shares_copy;
