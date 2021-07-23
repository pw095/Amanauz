INSERT
  INTO ref_calendar
  (
    tech$load_id,
    tech$load_dt,
    tech$record_source,
    clndr_dt,
    day_num_in_week,
    day_num_in_month,
    day_num_in_year,
    day_name,
    day_abbrev,
    holiday_flag,
    week_num_in_year,
    week_begin_date,
    month_num_in_year,
    month_name,
    month_abbrev,
    quarter,
    year
  )
SELECT
       :tech$load_id AS tech$load_id,
       tech$load_dt,
       tech$record_source,
       clndr_dt,
       CASE CAST(STRFTIME('%w', clndr_dt) AS INTEGER)
            WHEN 0 THEN
                7
            ELSE
                CAST(STRFTIME('%w', clndr_dt) AS INTEGER)
       END AS day_num_in_week,
       CAST(STRFTIME('%d', clndr_dt) AS INTEGER)     AS day_num_in_month,
       CAST(STRFTIME('%j', clndr_dt) AS INTEGER)     AS day_num_in_year,
       CASE CAST(STRFTIME('%w', clndr_dt) AS INTEGER)
            WHEN 1 THEN
                'ПОНЕДЕЛЬНИК'
            WHEN 2 THEN
                'ВТОРНИК'
            WHEN 3 THEN
                'СРЕДА'
            WHEN 4 THEN
                'ЧЕТВЕРГ'
            WHEN 5 THEN
                'ПЯТНИЦА'
            WHEN 6 THEN
                'СУББОТА'
            WHEN 0 THEN
                'ВОСКРЕСЕНЬЕ'
       END AS day_name,
       CASE CAST(STRFTIME('%w', clndr_dt) AS INTEGER)
            WHEN 1 THEN
                'ПН'
            WHEN 2 THEN
                'ВТ'
            WHEN 3 THEN
                'СР'
            WHEN 4 THEN
                'ЧТ'
            WHEN 5 THEN
                'ПТ'
            WHEN 6 THEN
                'СБ'
            WHEN 0 THEN
                'ВС'
       END AS day_abbrev,
       holiday_flag,
       CAST(STRFTIME('%W', clndr_dt) AS INTEGER)                   AS week_num_in_year,
       DATE(clndr_dt, '-' || STRFTIME('%w', clndr_dt) || ' days') AS week_begin_date,
       CAST(STRFTIME('%m', clndr_dt) AS INTEGER)                   AS month_num_in_year,
       CASE CAST(STRFTIME('%m', clndr_dt) AS INTEGER)
            WHEN 1 THEN
                'ЯНВАРЬ'
            WHEN 2 THEN
                'ФЕВРАЛЬ'
            WHEN 3 THEN
                'МАРТ'
            WHEN 4 THEN
                'АПРЕЛЬ'
            WHEN 5 THEN
                'МАЙ'
            WHEN 6 THEN
                'ИЮНЬ'
            WHEN 7 THEN
                'ИЮЛЬ'
            WHEN 8 THEN
                'АВГУСТ'
            WHEN 9 THEN
                'СЕНТЯБРЬ'
            WHEN 10 THEN
                'ОКТЯБРЬ'
            WHEN 11 THEN
                'НОЯБРЬ'
            WHEN 12 THEN
                'ДЕКАБРЬ'
       END AS month_name,
       CASE CAST(STRFTIME('%m', clndr_dt) AS INTEGER)
            WHEN 1 THEN
                'ЯНВ'
            WHEN 2 THEN
                'ФЕВ'
            WHEN 3 THEN
                'МАР'
            WHEN 4 THEN
                'АПР'
            WHEN 5 THEN
                'МАЙ'
            WHEN 6 THEN
                'ИЮН'
            WHEN 7 THEN
                'ИЮЛ'
            WHEN 8 THEN
                'АВГ'
            WHEN 9 THEN
                'СЕН'
            WHEN 10 THEN
                'ОКТ'
            WHEN 11 THEN
                'НОЯ'
            WHEN 12 THEN
                'ДЕК'
       END AS month_abbrev,
       CEIL((CAST(STRFTIME('%m', clndr_dt) AS REAL) / 3)) AS quarter,
       CAST(STRFTIME('%Y', clndr_dt) AS INTEGER)          AS year
  FROM tech$ref_calendar src
 WHERE NOT EXISTS(SELECT
                         NULL
                    FROM ref_calendar sat
                   WHERE sat.clndr_dt = src.clndr_dt
                     AND sat.holiday_flag = src.holiday_flag)
ON CONFLICT(clndr_dt)
DO UPDATE
   SET tech$load_id = excluded.tech$load_id,
       tech$load_dt = excluded.tech$load_dt,
       tech$record_source = excluded.tech$record_source,
       holiday_flag = excluded.holiday_flag
