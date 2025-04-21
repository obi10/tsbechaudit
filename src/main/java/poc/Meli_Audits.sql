SELECT * FROM json_rows_table
/
select * from audit_tab;
/
create unique index pk_audit_tab 
  on audit_tab (ts, id) local;
--agregar constraint
alter table audit_tab
 add constraint pk_audit_tab primary 
  key (id)
 using index pk_audit_tab;
/
alter session set nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF6';
/
INSERT INTO AUDIT_TAB (ts, id, payload) VALUES (SYSTIMESTAMP, 1234, '{ "nombre": "Juan" }');
/
select * from audit_tab;
/
DELETE FROM audit_tab
where ID <> 1234


