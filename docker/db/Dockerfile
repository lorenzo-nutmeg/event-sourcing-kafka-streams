FROM mysql:5.7
# Based on https://github.com/debezium/docker-images/blob/master/examples/mysql/0.9/Dockerfile

COPY mysql.cnf /etc/mysql/conf.d/
COPY *.sql /docker-entrypoint-initdb.d/
