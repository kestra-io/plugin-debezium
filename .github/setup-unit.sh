# mkdir certs
# openssl req -new -x509 -days 365 -nodes -out certs/ca.crt -keyout certs/ca.key -subj "/CN=root-ca"

# mkdir certs/server
# openssl genrsa -des3 -out certs/server/server.key -passout pass:p4ssphrase 2048
# openssl rsa -in certs/server/server.key -passin pass:p4ssphrase -out certs/server/server.key
# openssl req -new -nodes -key certs/server/server.key -out certs/server/server.csr -subj "/CN=postgresql"
# openssl x509 -req -in certs/server/server.csr -days 365 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server/server.crt
# sudo chmod -R 600 certs/server/
# sudo chown -R 1001 certs/server/

# mkdir certs/client
# openssl genrsa -des3 -out certs/client/client.key -passout pass:p4ssphrase 2048
# openssl rsa -in certs/client/client.key -passin pass:p4ssphrase -out certs/client/client-no-pass.key
# openssl req -new -nodes -key certs/client/client.key -passin pass:p4ssphrase -out certs/client/client.csr -subj "/CN=postgres"
# openssl x509 -req -in certs/client/client.csr -days 365 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/client/client.crt

# mkdir plugin-debezium-postgres/src/test/resources/ssl/
# cp certs/client/* plugin-debezium-postgres/src/test/resources/ssl/
# cp certs/ca.crt plugin-debezium-postgres/src/test/resources/ssl/

docker compose -f docker-compose-ci.yml up --quiet-pull -d --wait
docker compose -f docker-compose-ci.yml exec mysql sh -c "mysql -u root -pmysql_passwd < /tmp/docker/mysql.sql"
docker compose -f docker-compose-ci.yml exec postgres  sh -c "export PGPASSWORD=pg_passwd && psql -d postgres -U postgres -f /tmp/docker/postgres.sql > /dev/null"
docker run -v ${PWD}/data:/tmp/docker --network=plugin-debezium_default mcr.microsoft.com/mssql-tools sh -c  "/opt/mssql-tools/bin/sqlcmd -S sqlserver -U sa -P Sqls3rv3r_Pa55word! -i /tmp/docker/sqlserver.sql"
