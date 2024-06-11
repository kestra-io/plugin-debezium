#!/bin/sh

# Set archive log mode and enable GG replication
ORACLE_SID=XE
export ORACLE_SID
sqlplus /nolog <<- EOF
	CONNECT sys/passwd AS SYSDBA
	alter system set db_recovery_file_dest_size = 10G;
	alter system set db_recovery_file_dest = '/opt/oracle/oradata/XE' scope=spfile;
	shutdown immediate
	startup mount
	alter database archivelog;
	alter database open;
        -- Should show "Database log mode: Archive Mode"
	archive log list
	exit;
EOF

# Enable Log Miner
sqlplus sys/passwd@XE as sysdba <<- EOF
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
  ALTER PROFILE DEFAULT LIMIT FAILED_LOGIN_ATTEMPTS UNLIMITED;
  exit;
EOF

# Create Log Miner Tablespace and User
sqlplus sys/passwd@XE as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/XE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF

# Create Log Miner Tablespace and User
sqlplus sys/passwd@XEPDB1 as sysdba <<- EOF
  CREATE TABLESPACE LOGMINER_TBS DATAFILE '/opt/oracle/oradata/XE/XEPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
  exit;
EOF

sqlplus sys/passwd@XE as sysdba <<- EOF
    create user c##dbzuser identified by dbz default tablespace LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS CONTAINER=ALL;
    GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
    GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$DATABASE TO c##dbzuser CONTAINER=ALL;
    GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
    GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
    GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;

    GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
    GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
    GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;

    GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
    GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;

    GRANT SELECT ON V_\$LOG TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGFILE TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$TRANSACTION TO c##dbzuser CONTAINER=ALL;
    exit;
EOF