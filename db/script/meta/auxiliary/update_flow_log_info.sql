UPDATE tbl_flow_log
   SET flow_status = :flow_status,
       flow_finish_dttm = :flow_finish_dttm
 WHERE flow_id = :flow_id
