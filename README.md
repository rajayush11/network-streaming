# network-streaming

Steps to execute Docker Part :
  1. Save the .yml file from the repo.
  2. Run docker-compose up command from the file where .yml file is stored in terminal
  3. docker-compose ps  for checking what all services are up
  4. FOR KAFKA USING KAFKA CLI - docker ps for container ID , 
  5. docker exec -it <container_id> /bin/bash for running kafka
  6. kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1  for creating topic. 