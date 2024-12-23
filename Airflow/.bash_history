ls
cd home
sudo ls
ls
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y install xfce4
sudo apt-get -y install xrdp
sudo systemctl enable xrdp
sudo adduser xrdp ssl-cert
echo xfce4-session >~/.xsession
sudo service xrdp restart
az vm open-port --resource-group DEFI-BFT --name ML-Airflow-Server --port 3389
ls
ls
touch hello
ls
cd opt
cd /opt/
ls
cd containerd
ls
sudo bash
cd my_airflow
sudo bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo su - postgres
pip install psycopg2
sudo apt-get install libpq-dev python-dev
pip install psycopg2
ls
sudo bash
ls
sudo bash
pip install beautifulsoup4
sudo bash
psql "postgresql+psycopg2://airflow:airflow@postgres/airflow"
psql postgresql://airflow:airflow@postgres/airflow
sudo bash
psql postgresql://airflow:airflow@postgres/airflow
psql -U $(whoami) -d postgres
sudo -i -u postgres
sudo apt-get remove postgresql postgresql-contrib
sudo apt-get autoremove
sudo apt-get autoclean
psql postgresql://airflow:airflow@localhost/airflow
psql
sudo apt-get --purge remove postgresql postgresql-*
psql
psql postgresql://airflow:airflow@localhost/airflow
sudo apt-get update && sudo apt-get install postgresql-client
psql postgresql://airflow:airflow@localhost/airflow
psql postgresql://airflow:airflow@postgres/airflow
sudo yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
sudo apt update && sudo apt -y full-upgrade
sudo reboot
sudo apt install curl gpg gnupg2 software-properties-common apt-transport-https lsb-release ca-certificates
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc|sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install postgresql-13 postgresql-client-13
psql
sudo su - postgres
psql postgresql://airflow:airflow@localhost/airflow
psql postgresql://:root:Edinburgh123@localhost/airflow
psql postgresql://root:Edinburgh123@localhost/airflow
psql postgresql://root:Edinburgh123@localhost
psql postgresql://root:Edinburgh123@postgres
psql postgresql://root:Edinburgh123@localhost
psql
sudo bash
psql
psql postgresql://airflow:airflow@localhost:5432/airflow
pip install yfinance
sudo apt-get install python3-pip
pip3 install yfinance
sudo apt-get update
sudo apt-get install libssl-dev
pip uninstall yfinance
pip install yfinance
pip install --upgrade pip
pip uninstall pip
pip check
python3 -m venv myenv
source myenv/bin/activate
python -m ensurepip --upgrade
deactivate
pip check
pip install pip --upgrade
sudo apt remove python3-pip
wget https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py
pip install pyopenssl --upgrade
which pip
/usr/local/bin/pip install pyopenssl --upgrade
pip install yfinance
nano ~/.bashrc
nano ~/.bash_profile
source ~/.bash_profile
pip install yfinance
cd /home/my_airflow/dags
python draft_workflow2.py
docker ps
sudo bash
docker exec -it my_airflow-airflow-webserver-1 bash
sudo bash
ls
cd /home/my_airflow/dags/visualisation-web-infrastructure
ls
cd MiniProject
ls
cd static
ls
cd ..
cd templates
ls
cd home
cd /home
ls
mkdir models
cd ..
ls
mkdir models
cd /home
sudo bash
sudo bash
docker --version
cd /home/my_airflow
ls
docker-compose up
docker ps
sudo bash
cd /home/my_airflow/
ls
nano docker-compose.yaml
docker --version
docker compose version
docker compose version
docker ps
sudo bash
docker ps
sudo bash
ls
cd ..
ls
cd my_airflow
ls
cd newdocker
ls
sudo bash
ls
cd ..
cd my_airflow
ls
cd newdocker
ls
cd ..
rm -r newdocker
ls
rm newdocker
rm -r newdocker
ls
mkdir newdocker
sudo chown -R airflow:airflow /home/my_airflow/
sudo bash
sudo bash
sudo bash
sudo bash
sudo bash
sudo bash
cd ..
sudo bash
docker logs airflow_container
sudo bash
sudo bash
sudo docker ps
container logs airflow_container
sudo bash
docker-compose --version
sudo bash
sudo bash
sudo docker system prune -a
sudo apt-get autoremove
cd..
ls
cd ..
ls
rm -r Hailey_Final_Project
sudo apt-get autoremove
rm -r data_cleanning
cd models
ls
cd ..
ls
sudo apt-get autoremove
cd my_airflow
ls
rm -r newdocker
sudo apt-get autoremove
sudo apt --fix-broken install
sudo apt-get clean
sudo nano /etc/hosts
sudo reboot
sudo bash
sudo bash
sudo bash
sudo mkfs.ext4 /dev/sdc
sudo mkdir /dockerDataDrive
sudo mount /dev/sdc /dockerDataDrive
UUID=$(blkid -s UUID -o value /dev/sdc)
echo "UUID=${UUID} /dockerDataDrive ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
sudo blkid /dev/sdc
echo 'UUID=f100f1a1-fb77-4030-940f-932ad9907caf /dockerDataDrive ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
sudo umount /dockerDataDrive
sudo mount -a
df -h /dockerDataDrive
ls
cd ..
ls
cd ..
ls
cd docker DataDrive
cd dockerDataDrive
ls
cd lost+found
sudo bash
df -h
df -h /dockerDataDrive
docker ps
sudo bash
docker ps
sudo bash
sudo bash
ls
ls data
ls airflow_docker/
ls
cd ..
ls
cd ..
ls
cd home
ls
useradd –d  /home/test1 -m test1
passwd test1
useradd -m -d /home/test1 test1
sudo useradd -m -d /home/test1 test1
sudo passwd test1
ls
ls
sudo useradd -m -d /home/test2 test2
sudo passwd test2
