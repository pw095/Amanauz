@field_name@ = CASE
                  WHEN tech$expiration_dt = '2999-12-31'
                   AND excluded.tech$expiration_dt = '2999-12-31' THEN
                      excluded.@field_name@
                  ELSE
                      @field_name@
             END