SELECT
       fll_name,
       fll_hash_sum
  FROM (SELECT
               fll_name,
               fll_hash_sum,
               ROW_NUMBER() OVER (PARTITION BY fll_name ORDER BY fll_ell_id DESC) AS rn
          FROM tbl_file_load_log fll
         WHERE EXISTS(SELECT
                             NULL
                        FROM tbl_entity_load_log ell
                       WHERE ell.ell_id = fll.fll_ell_id
                         AND ell.ell_elm_id = :ell_elm_id))
 WHERE rn = 1
