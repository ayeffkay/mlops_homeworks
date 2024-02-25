ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa <<<y >/dev/null 2>&1
AIRFLOW_DIR=/opt/airflow
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | \
        bash -s -- -i $AIRFLOW_DIR -n && $AIRFLOW_DIR/bin/yc init
source ~/.bashrc
