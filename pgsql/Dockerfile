FROM postgres:13
ENV POSTGRES_PASSWORD=postgres
COPY ./init_system.sh /docker-entrypoint-initdb.d/
EXPOSE 5432
