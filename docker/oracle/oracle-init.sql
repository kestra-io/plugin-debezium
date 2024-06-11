declare
  e_not_exists exception;
  pragma exception_init(e_not_exists, -1918);
begin
  execute immediate 'drop user kestra cascade';
exception
  when e_not_exists then null;
end;
/

CREATE USER kestra IDENTIFIED BY passwd;
GRANT CONNECT, RESOURCE TO kestra;

ALTER SESSION SET CURRENT_SCHEMA = kestra;

ALTER USER kestra quota unlimited on USERS;