AIRFLOW_VM_USER_NAME=yc-user
AIRFLOW_VM_NAME=$(shell head -1 ../airflow_vm_metadata.txt)
AIRFLOW_VM_IP=$(shell tail -1 ../airflow_vm_metadata.txt)
AIRFLOW_VM_PORT=8080

airflow.copy_files:
	scp -r ../scripts $(AIRFLOW_VM_USER_NAME)@$(AIRFLOW_VM_IP):~/
	scp -r ../dags $(AIRFLOW_VM_USER_NAME)@$(AIRFLOW_VM_IP):~/
airflow.install_docker:
	ssh $(AIRFLOW_VM_USER_NAME)@$(AIRFLOW_VM_IP) -t "sudo apt update && sudo apt install -y make && mv scripts/Makefile . && bash ./scripts/install_docker.sh"
airflow.prepare_docker:
	ssh $(AIRFLOW_VM_USER_NAME)@$(AIRFLOW_VM_IP) -t "mv ./scripts/docker-compose.yaml . && bash ./scripts/prepare_airflow_docker.sh && docker compose up -d && exit"
airflow.forward_ui_port:
	ssh $(AIRFLOW_VM_USER_NAME)@$(AIRFLOW_VM_IP) -L $(AIRFLOW_VM_PORT):$(AIRFLOW_VM_IP):$(AIRFLOW_VM_PORT)
airflow.destroy_vm:
	yc compute instance delete --name $(AIRFLOW_VM_NAME)
local.run_airflow_ui:
	python3 -m webbrowser "http://localhost:8080" &
airflow.root_shell:
	docker exec -u root -it yc-user-airflow-webserver-1 bash
	sudo chmod 775 /etc
airflow.shell:
	docker exec -it yc-user-airflow-webserver-1 bash
airflow.prepare_for_spark:
	chmod -R -f 777  ~/scripts/*.sh && bash ~/scripts/prepare_for_spark.sh
airflow.restart_server:
	docker compose restart
airflow.stop_server:
	docker compose stop
airflow.destroy_server:
	docker compose down