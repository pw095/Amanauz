select * from ref_calendar;
SELECT DATE('NOW');
SELECT STRFTIME('%j', DATE('NOW'))
SELECT STRFTIME('%d', 'NOW');

select * from ref_calendar where clndr_dt in ('2017-03-08', '2018-03-08', '2019-03-08', '2020-03-08');

select * from master_data_ref_calendar WHERE FULL_DATE = '2018-03-08';
update master_data_ref_calendar set tech$hash_value = '1a44b15ff296a8c0777b7000da1b83443708b085', holiday_flag = 'Выходной' WHERE FULL_DATE = '2017-03-08';
select * from master_data_ref_calendar WHERE FULL_DATE = '2020-03-08';

INSERT
  INTO ref_calendar
  (
    tech$load_id,
    clndr_dt,
    tech$load_dt,
    tech$record_source,
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
       :tech$load_id     AS tech$load_id,
       full_date         AS clndr_dt,
       tech$effective_dt AS tech$load_dt,
       'master_data'     AS tech$record_source,
       CASE CAST(STRFTIME('%w', full_date) AS INTEGER)
            WHEN 0 THEN
                7
            ELSE
                CAST(STRFTIME('%w', full_date) AS INTEGER)
       END AS day_num_in_week,
       CAST(STRFTIME('%d', full_date) AS INTEGER)     AS day_num_in_month,
       CAST(STRFTIME('%j', full_date) AS INTEGER)     AS day_num_in_year,
       CASE CAST(STRFTIME('%w', full_date) AS INTEGER)
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
       CASE CAST(STRFTIME('%w', full_date) AS INTEGER)
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
       CAST(STRFTIME('%W', full_date) AS INTEGER)                   AS week_num_in_year,
       DATE(full_date, '-' || STRFTIME('%w', full_date) || ' days') AS week_begin_date,
       CAST(STRFTIME('%m', full_date) AS INTEGER)                   AS month_num_in_year,
       CASE CAST(STRFTIME('%m', full_date) AS INTEGER)
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
       CASE CAST(STRFTIME('%m', full_date) AS INTEGER)
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
       CEIL((CAST(STRFTIME('%m', full_date) AS REAL) / 3)) AS quarter,
       CAST(STRFTIME('%Y', full_date) AS INTEGER)          AS year
  FROM master_data_ref_calendar src
 WHERE NOT EXISTS(SELECT
                         NULL
                    FROM ref_calendar sat
                   WHERE sat.clndr_dt = src.full_date
                     AND sat.holiday_flag = src.holiday_flag)
   AND tech$expiration_dt = '2999-12-31'
   AND tech$effective_dt >= :tech$effective_dt
ON CONFLICT(clndr_dt)
DO UPDATE
   SET tech$load_id = excluded.tech$load_id,
       tech$load_dt = excluded.tech$load_dt,
       holiday_flag = excluded.holiday_flag;








SELECT * FROM master_data_ref_calendar WHERE tech$expiration_dt = '2999-12-31';
SELECT
       STRFTIME('%w', 'NOW') AS day_num_in_week,
       STRFTIME('%d', 'NOW') AS day_num_in_month,
       STRFTIME('%j', 'NOW') AS day_num_in_year,
       CASE CAST(STRFTIME('%w', 'NOW') AS INTEGER)
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
            WHEN 7 THEN
                'ВОСКРЕСЕНЬЕ'
       END AS day_name,
       CASE CAST(STRFTIME('%w', 'NOW') AS INTEGER)
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
            WHEN 7 THEN
                'ВС'
       END AS day_abbrev,
       null AS weekday_flag,
       STRFTIME('%W', 'NOW') AS week_num_in_year,
       DATE('NOW', '-' || STRFTIME('%w', 'NOW') || ' days') AS week_begin_date,
       STRFTIME('%m', 'NOW') AS month_num_in_year,
         CASE CAST(STRFTIME('%m', 'NOW') AS INTEGER)
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
       CASE CAST(STRFTIME('%m', 'NOW') AS INTEGER)
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
       CEIL((CAST(STRFTIME('%m', 'NOW') AS REAL) / 3)) AS quarter,
       STRFTIME('%Y', 'NOW') AS year;