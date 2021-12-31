#!/bin/bash

# VARIABLES
IMG_NAME="spark-hadoop-cluster"
HOST_PREFIX="cluster"
NETWORK_NAME=$HOST_PREFIX

N=${1:-2}
NET_QUERY=$(docker network ls | grep -i $NETWORK_NAME)
if [ -z "$NET_QUERY" ]; then
	docker network create --driver=bridge $NETWORK_NAME
fi

# START HADOOP SLAVES 
i=1
while [ $i -le $N ]
do
	HADOOP_SLAVE="$HOST_PREFIX"-slave-$i
	docker run --name $HADOOP_SLAVE -h $HADOOP_SLAVE --net=$NETWORK_NAME -itd "$IMG_NAME"
	i=$(( $i + 1 ))
done

# START HADOOP MASTER

HADOOP_MASTER="$HOST_PREFIX"-master
docker run --name $HADOOP_MASTER -h $HADOOP_MASTER --net=$NETWORK_NAME \
		-p  8088:8088  -p 50070:50070 -p 50090:50090 \
		-p  8080:8080 \
		-v "app:/app" \
		-itd "$IMG_NAME"


# START MULTI-NODES CLUSTER
docker exec -it $HADOOP_MASTER "/usr/local/hadoop/spark-services.sh"

# START RABBITMQ
docker run --name rabbitmq -h rabbitmq --net=$NETWORK_NAME \
	-p 5672:5672 -p 15672:15672 -p 1883:1883 \
	-itd rabbit_mqtt

# START ELASTICSEARCH - KIBANA
docker run --name elasticsearch -h elasticsearch --net=$NETWORK_NAME \
	-v ./elasticsearch/data:/usr/share/elasticsearch/data \
	-itd docker.elastic.co/elasticsearch/elasticsearch:7.9.2-amd64

docker run --name kibana -h kibana --net=$NETWORK_NAME \
	-p 5601:5601 \
	-itd docker.elastic.co/kibana/kibana:7.9.2


