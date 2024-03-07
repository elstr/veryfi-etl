# Start Airflow and PostgreSQL containers and then run the webserver
start:
		@make up && \
		CONTAINER_NAME=$$(docker-compose ps -q airflow) make webserver

# Run the Airflow webserver in the container
webserver:
		docker exec -it $(CONTAINER_NAME) airflow webserver

# Build and start Airflow and PostgreSQL containers
up:
		docker-compose --profile flower up -d

# Stop and remove Airflow and PostgreSQL containers
down:
		docker-compose down

# Clean up volumes and networks
clean:
		docker-compose down -v --remove-orphans
