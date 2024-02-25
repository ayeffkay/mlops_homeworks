#!/bin/bash
export AIRFLOW_VM_NAME="airflow"
export AIRFLOW_VM_USER_NAME="yc-user"

yc compute instance create --name $AIRFLOW_VM_NAME \
                            --description "Airflow server" \
                            --zone "ru-central1-b" \
                            --ssh-key "/home/alina/.ssh/id_rsa.pub" \
                            --public-ip \
                            --create-boot-disk image-folder-id=standard-images,`
                                               `image-family=ubuntu-2204-lts,` 
                                              `type=network-hdd,`
                                              `size=32,`
                                              `auto-delete=true \
                            --memory 8 \
                            --cores 8 \
                            --core-fraction 100 \
                            --metadata serial-port-enable=1 \
                            --preemptible > ../airflow_vm_full_metadata.txt

vm_ip=$(yc compute instance list | grep $AIRFLOW_VM_NAME | awk -F "|" '{print $6}' | sed 's/^[ \t]*//;s/[ \t]*$//')
export AIRFLOW_VM_IP=$vm_ip

echo "AIRFLOW VM $AIRFLOW_VM_USER_NAME@$AIRFLOW_VM_IP WAS CREATED!"
echo -e "$AIRFLOW_VM_NAME\n$AIRFLOW_VM_IP" > ../airflow_vm_metadata.txt

ssh -o StrictHostKeyChecking=no -l $AIRFLOW_VM_USER_NAME $AIRFLOW_VM_IP -t "exit"