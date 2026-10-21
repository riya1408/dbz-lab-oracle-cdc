#!/bin/bash
# Runs automatically EVERY time the Oracle container starts (from /opt/oracle/scripts/startup).
# It is idempotent: if the Debezium user already exists, it does nothing.
# Prepares Oracle for Debezium (LogMiner) and creates the demo bank schema.
#
# Oracle's startup runner "sources" this file, so everything runs inside ( ... ):
# a subshell, so 'exit' and 'set -e' can't affect the container's own scripts.
(
set -e

ALREADY=$(sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT COUNT(*) FROM dba_users WHERE username = 'C##DBZUSER';
EXIT;
SQL
)
if [ "$(echo "$ALREADY" | tr -d '[:space:]')" != "0" ]; then
  echo ">>> [cdc-setup] Already configured - skipping."
  exit 0
fi


echo ">>> [cdc-setup] 1/4 Enabling ARCHIVELOG mode and supplemental logging"
sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
EXIT;
SQL

echo ">>> [cdc-setup] 2/4 Creating LogMiner tablespaces"
sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
EXIT;
SQL

echo ">>> [cdc-setup] 3/4 Creating Debezium user c##dbzuser (least privilege for LogMiner)"
sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT FAILURE
CREATE USER c##dbzuser IDENTIFIED BY "${DBZ_PASSWORD}"
  DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
EXIT;
SQL

sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL;
EXIT;
SQL

echo ">>> [cdc-setup] 4/4 Creating bank schema and demo customers"
sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT FAILURE
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER bank IDENTIFIED BY "${BANK_PASSWORD}" QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE TO bank;
CREATE TABLE bank.customer (
  customer_id    NUMBER PRIMARY KEY,
  name           VARCHAR2(100),
  city           VARCHAR2(50),
  segment        VARCHAR2(20),
  annual_income  NUMBER
);
ALTER TABLE bank.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
INSERT INTO bank.customer VALUES (1, 'Asha',  'Delhi',  'Premium',  1800000);
INSERT INTO bank.customer VALUES (2, 'Ravi',  'Mumbai', 'Standard',  900000);
INSERT INTO bank.customer VALUES (3, 'Meena', 'Pune',   'Standard',  750000);
COMMIT;
EXIT;
SQL

echo ">>> [cdc-setup] Done. Oracle is ready for Debezium."
)
