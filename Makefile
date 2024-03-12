# Start Airflow and PostgreSQL containers and then run the webserver
start:
		@make up

# Build and start Airflow and PostgreSQL containers
up:
		docker-compose up

# Stop and remove Airflow and PostgreSQL containers
down:
		docker-compose down

# Clean up volumes and networks
clean:
		docker-compose down -v --remove-orphans
