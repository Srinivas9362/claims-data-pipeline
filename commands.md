ssh -i "D:\trendy tech project\claims-airflow-key.pem" ec2-user@13.220.2.145

whoami
hostname
pwd
ls -la ~/.ssh/


cd ~/claims-data-pipeline
docker compose ps
docker ps

sudo ss -lntp | grep 8080

curl -I http://localhost:8080

docker compose config --services

docker compose ps -a
docker compose up -d postgres

docker compose restart airflow-api-server

[ec2-user@ip-172-31-9-37 claims-data-pipeline]$ cat .env
AIRFLOW__CORE__FERNET_KEY=gOT2uX8E7n_ZUrZiBlvWFdG181ctEdXLRcRQEkJdAxY=
AIRFLOW_JWT_SECRET=961b0f895fbc5ce74e0a19678f622b345c06d773d42e059e5b5f3568824d0821


#to know the password

docker compose exec airflow-api-server \
cat /opt/airflow/simple_auth_manager_passwords.json.generated 

admin
zxQYxfRmtzKc6PK5