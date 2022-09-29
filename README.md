# spark-es-django

## Obtener IP elasticsearch
docker ps
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_id

## Setear IP en Spark y Django
Spark: spark/home/ES_Spark_Recommendation.py
Django: django/home/demo/demo/views.py

## Ejecutar Spark
cd /home
python3 ES_Spark_Recommendation.py

## Levantar Django
cd /home/demo
python3 manage.py runserver 0:8000
