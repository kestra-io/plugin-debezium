
# Set archive log mode and enable GG replication
ORACLE_SID=XE
export ORACLE_SID


mkdir -p /opt/oracle/oradata/recovery_area

sqlplus /nolog <<- EOF
	CONNECT sys/oraclepasswd AS SYSDBA
	alter system set db_recovery_file_dest_size = 10G;
	alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
	shutdown immediate
	startup mount
	alter database archivelog;
	alter database open;
        -- Should show "Database log mode: Archive Mode"
	archive log list
	exit;
EOF

# Enable LogMiner required database features/settings
sqlplus sys/oraclepasswd@//localhost:1521/XE as sysdba <<- EOF
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
  ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
  exit;
EOF

# Create Log Miner Tablespace and User
sqlplus sys/oraclepasswd@//localhost:1521/XE as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/XE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF

sqlplus sys/oraclepasswd@//localhost:1521/XEPDB1 as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/XE/XEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF


sqlplus sys/oraclepasswd@//localhost:1521/XE as sysdba <<- EOF
  CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
  GRANT CREATE SESSION TO c##dbzuser;
  GRANT SELECT ON V_\$DATABASE TO c##dbzuser;
  GRANT FLASHBACK ANY TABLE TO c##dbzuser;
  GRANT SELECT ANY TABLE TO c##dbzuser;
  GRANT SELECT_CATALOG_ROLE TO c##dbzuser;
  GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser;
  GRANT SELECT ANY TRANSACTION TO c##dbzuser;
  GRANT SELECT ANY DICTIONARY TO c##dbzuser;
  GRANT LOGMINING TO c##dbzuser;
  GRANT CREATE TABLE TO c##dbzuser;
  GRANT LOCK ANY TABLE TO c##dbzuser;
  GRANT CREATE SEQUENCE TO c##dbzuser;
  GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser;
  GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser;
  GRANT SELECT ON V_\$LOGMNR_LOGS to c##dbzuser;
  GRANT SELECT ON V_\$LOGMNR_CONTENTS TO c##dbzuser;
  GRANT SELECT ON V_\$LOGFILE TO c##dbzuser;
  GRANT SELECT ON V_\$ARCHIVED_LOG TO c##dbzuser;
  GRANT SELECT ON V_\$ARCHIVE_DEST_STATUS TO c##dbzuser;
  exit;
EOF

sqlplus sys/oraclepasswd@//localhost:1521/XEPDB1 as sysdba <<- EOF
  CREATE USER debezium IDENTIFIED BY dbz;
  GRANT CONNECT TO debezium;
  GRANT CREATE SESSION TO debezium;
  GRANT CREATE TABLE TO debezium;
  GRANT CREATE SEQUENCE to debezium;
  ALTER USER debezium QUOTA 100M on users;
  exit;
EOF
