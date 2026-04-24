# СОА ДЗ 5

`sudo docker compose up --build`
`sudo docker compose down -v`
`sudo docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic movie-events --from-beginning`
`sudo docker compose exec clickhouse clickhouse-client --password clickhouse`
`SELECT * FROM movie_events;`
