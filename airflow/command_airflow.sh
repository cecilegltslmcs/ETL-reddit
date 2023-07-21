# Init database
airflow db init

# Create user to access UI
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# Initialize scheduler
airflow scheduler

# Start webserver & UI
airflow webserver