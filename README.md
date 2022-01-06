# Foody

## Khởi động container
```
cd Foody
docker-compose up -d
```

## Khởi động spark cluster

Attach shell vào master 
```
bash /usr/spark-2*/sbin/start-master.sh
```

Attach shell 2 slave
```
bash /usr/spark-2*/sbin/start-slave.sh $MASTER
```

## Run a spark job
```
spark-submit /app/job-name.py
```
