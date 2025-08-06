sudo systemctl daemon-reload
sudo systemctl enable airflow-scheduler
sudo systemctl enable airflow-webserver
sudo systemctl start airflow-scheduler
sudo systemctl start airflow-webserver

systemctl status airflow-scheduler
systemctl status airflow-webserver
systemctl status mysql
