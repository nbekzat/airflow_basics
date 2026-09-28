### Deploying Airflow's Componenets ###

# Database
airflow db migrate

# create user
airflow users create \
    --username admin \
    --password admin \
    --firstname bek \
    --lastname zat \
    --role Admin \
    --email bekzat123@gmail.com