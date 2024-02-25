#!/bin/bash
CLUSTER_NAME=dataproc9df501bd
CLUSTER_METADATA="cluster_metadata.txt"
CLUSTER_METADATA_FULL="cluster_metadata_full.txt"
export CLUSTER_NAME
export CLUSTER_METADATA
export CLUSTER_METADATA_FULL

/opt/airflow/bin/yc config set token xxx_generated_oauth_token_xxx
/opt/airflow/bin/yc dataproc cluster create --name=$CLUSTER_NAME \
                            --description="Test cluster for homework" \
                            --zone=ru-central1-b \
                            --service-account-name=otus-mlops \
                            --version=2.0 \
                            --services=YARN,SPARK \
                            --ssh-public-keys-file="/home/airflow/.ssh/id_rsa.pub" \
                            --subcluster name=dataproc9df501bd_subcluster42c7,`
                                         `role=masternode,`
                                         `resource-preset=s3-c2-m8,`
                                         `disk-type=network-ssd,`
                                         `disk-size=40,`
                                         `subnet-name=default-ru-central1-b,`
                                         `hosts-count=1,`
                                         `assign-public-ip=true \
                            --subcluster name=dataproc9df501bd_subcluster883c,`
                                        `role=computenode,`
                                        `resource-preset=s3-c4-m16,`
                                        `disk-type=network-hdd,`
                                        `disk-size=128,`
                                        `subnet-name=default-ru-central1-b,`
                                        `hosts-count=4,`
                                        `assign-public-ip=true \
                            --security-group-ids=enp4n8cs1pm2i0c54bi3 \
                            --ui-proxy=true \
                            --bucket=small-bucket > $CLUSTER_METADATA_FULL

cluster_id=$(/opt/airflow/bin/yc dataproc cluster list | awk -F "|" '{print $2}' | cut -d' ' -f2 | grep "\S")
masternode_id=$(/opt/airflow/bin/yc dataproc cluster list-hosts --id $cluster_id | grep MASTERNODE | awk -F "|" '{print $3}')
masternode_ip=$(/opt/airflow/bin/yc compute instance list | grep $masternode_id | awk -F "|" '{print $6}' | sed 's/^[ \t]*//;s/[ \t]*$//')

export CLUSTER_ID=$cluster_id
export MASTERNODE_IP=$masternode_ip

echo -e "$CLUSTER_NAME\n$CLUSTER_ID\n$MASTERNODE_IP" > $CLUSTER_METADATA

ssh -o StrictHostKeyChecking=no -l ubuntu $MASTERNODE_IP -t "sudo pip install s3cmd && pip install findspark && exit"

echo $MASTERNODE_IP
