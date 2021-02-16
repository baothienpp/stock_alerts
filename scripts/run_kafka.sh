mkdir -p ~/kafka-data
mkdir -p ~/kafka-data/logs
mkdir -p ~/kafka-data/settings

#docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=172.17.0.1 --env ADVERTISED_PORT=9092 spotify/kafka
docker run -d -v ~/kafka-data/logs:/tmp/kafka-logs -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=172.17.0.1 --env ADVERTISED_PORT=9092 spotify/kafka


