mkdir -p ~/kafka-data
#docker run -d -v ~/kafka-data:/tmp/kafka-logs -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=172.17.0.1 --env ADVERTISED_PORT=9092 spotify/kafka
docker run -d -v ~/kafka-data:/tmp/kafka-logs -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 spotify/kafka
