FROM python:3.7


RUN mkdir /sensordataelt

COPY requirements.txt /sensordataelt/requirements.txt
RUN pip install -r /sensordataelt/requirements.txt 

COPY scripts_airflow/ /sensordataelt/scripts/
COPY scripts/Create_DWH.py /sensordataelt/scripts/Create_DWH.py

COPY sensor-data-dbt/profiles.yml /root/.dbt/profiles.yml 




RUN chmod +x /sensordataelt/scripts/init.sh
ENTRYPOINT [ "/sensordataelt/scripts/init.sh" ]