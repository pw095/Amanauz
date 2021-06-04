INSERT
  INTO security_boards
  (
    tech$load_id,
    tech$load_dttm,
    id,
    board_group_id,
    engine_id,
    market_id,
    board_id,
    board_title,
    is_traded,
    has_candles,
    is_primary
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :id,
    :board_group_id,
    :engine_id,
    :market_id,
    :board_id,
    :board_title,
    :is_traded,
    :has_candles,
    :is_primary
  )
