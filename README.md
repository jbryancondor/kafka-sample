# kafka
```
# While the Docker images themselves are supported for production usage,
# this docker-compose.yaml is designed to be used by developers to run
# an environment locally. It is not designed to be used in production.
# We recommend to use Kubernetes in production with our Helm Charts:
# https://docs.camunda.io/docs/self-managed/platform-deployment/kubernetes-helm/
# For local development, we recommend using KIND instead of `docker-compose`:
# https://docs.camunda.io/docs/self-managed/platform-deployment/kubernetes-helm/#installing-the-camunda-helm-chart-locally-using-kind

# This is a lightweight configuration with Zeebe, Operate, Tasklist, and Elasticsearch
# See docker-compose.yml for a configuration that also includes Optimize, Identity, and Keycloak.

services:

  zeebe: # https://docs.camunda.io/docs/self-managed/platform-deployment/docker/#zeebe
    image: docker.io/library/zeebe:latest
    container_name: zeebe
    ports:
      - "26500:26500"
      - "9600:9600"
    environment: # https://docs.camunda.io/docs/self-managed/zeebe-deployment/configuration/environment-variables/
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME=io.camunda.zeebe.exporter.ElasticsearchExporter
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL=http://elasticsearch:9200
      - ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_SIZE=1
      # allow running with low disk space
      - ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK=0.998
      - ZEEBE_BROKER_DATA_DISKUSAGEREPLICATIONWATERMARK=0.999
      - "JAVA_TOOL_OPTIONS=-Xms512m -Xmx512m"
    restart: always
    volumes:
      - zeebe:/usr/local/zeebe/data
    networks:
      - camunda-platform
    depends_on:
      - elasticsearch

  operate: # https://docs.camunda.io/docs/self-managed/platform-deployment/docker/#operate
    image: camunda/operate:${CAMUNDA_PLATFORM_VERSION:-8.0.6}
    container_name: operate
    ports:
      - "8091:8080"
    environment: # https://docs.camunda.io/docs/self-managed/operate-deployment/configuration/
      - CAMUNDA_OPERATE_ZEEBE_GATEWAYADDRESS=zeebe:26500
      - CAMUNDA_OPERATE_ELASTICSEARCH_URL=http://elasticsearch:9200
      - CAMUNDA_OPERATE_ZEEBEELASTICSEARCH_URL=http://elasticsearch:9200
    networks:
      - camunda-platform
    depends_on:
      - zeebe
      - elasticsearch

  tasklist: # https://docs.camunda.io/docs/self-managed/platform-deployment/docker/#tasklist
    image: camunda/tasklist:${CAMUNDA_PLATFORM_VERSION:-8.0.6}
    container_name: tasklist
    ports:
      - "8092:8080"
    environment: # https://docs.camunda.io/docs/self-managed/tasklist-deployment/configuration/
      - CAMUNDA_TASKLIST_ZEEBE_GATEWAYADDRESS=zeebe:26500
      - CAMUNDA_TASKLIST_ELASTICSEARCH_URL=http://elasticsearch:9200
      - CAMUNDA_TASKLIST_ZEEBEELASTICSEARCH_URL=http://elasticsearch:9200
    networks:
      - camunda-platform
    depends_on:
      - zeebe
      - elasticsearch

  elasticsearch: # https://hub.docker.com/_/elasticsearch
    image: docker.elastic.co/elasticsearch/elasticsearch:${ELASTIC_VERSION:-7.17.0}
    container_name: elasticsearch
    ports:
      - "9200:9200"
      - "9300:9300"
    environment:
      - bootstrap.memory_lock=true
      - discovery.type=single-node
      - xpack.security.enabled=false
      # allow running with low disk space
      - cluster.routing.allocation.disk.threshold_enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    restart: always
    healthcheck:
      test: [ "CMD-SHELL", "curl -f http://localhost:9200/_cat/health | grep -q green" ]
      interval: 30s
      timeout: 5s
      retries: 3
    volumes:
      - elastic:/usr/share/elasticsearch/data
    networks:
      - camunda-platform

  zookeeper:
    image: confluentinc/cp-zookeeper:7.2.2
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - camunda-platform

  broker:
    image: confluentinc/cp-server:7.2.2
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
      - "3031:3031"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: broker:29092
      CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      CONFLUENT_METRICS_ENABLE: 'true'
      CONFLUENT_SUPPORT_CUSTOMER_ID: 'anonymous'
    networks:
      - camunda-platform

  schema-registry:
    image: confluentinc/cp-schema-registry:7.2.2
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - camunda-platform

  connect:
    image: cnfldemos/cp-server-connect-datagen:0.6.0-7.2.1
    hostname: connect
    container_name: connect
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: 'broker:29092'
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      # CLASSPATH required due to CC-2422
      CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.2.2.jar
      CONNECT_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      CONNECT_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"
      CONNECT_LOG4J_LOGGERS: org.apache.zookeeper=ERROR,org.I0Itec.zkclient=ERROR,org.reflections=ERROR
      CONNECT_PLUGIN_PATH: '/usr/share/java,/etc/kafka-connect/jars'
    volumes:
      - ./connectors:/etc/kafka-connect/jars/
    networks:
      - camunda-platform

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.2.2
    hostname: control-center
    container_name: control-center
    depends_on:
      - broker
      - schema-registry
      - connect
      - ksqldb-server
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:29092'
      CONTROL_CENTER_CONNECT_CONNECT-DEFAULT_CLUSTER: 'connect:8083'
      CONTROL_CENTER_KSQL_KSQLDB1_URL: "http://ksqldb-server:8088"
      CONTROL_CENTER_KSQL_KSQLDB1_ADVERTISED_URL: "http://localhost:8088"
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      PORT: 9021
    networks:
      - camunda-platform

  ksqldb-server:
    image: confluentinc/cp-ksqldb-server:7.2.2
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - connect
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "broker:29092"
      KSQL_HOST_NAME: ksqldb-server
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_CACHE_MAX_BYTES_BUFFERING: 0
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_PRODUCER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
      KSQL_CONSUMER_INTERCEPTOR_CLASSES: "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
      KSQL_KSQL_CONNECT_URL: "http://connect:8083"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR: 1
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: 'true'
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: 'true'
    networks:
      - camunda-platform

  ksqldb-cli:
    image: confluentinc/cp-ksqldb-cli:7.2.2
    container_name: ksqldb-cli
    depends_on:
      - broker
      - connect
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
    networks:
      - camunda-platform

  ksql-datagen:
    image: confluentinc/ksqldb-examples:7.2.2
    hostname: ksql-datagen
    container_name: ksql-datagen
    depends_on:
      - ksqldb-server
      - broker
      - schema-registry
      - connect
    command: "bash -c 'echo Waiting for Kafka to be ready... && \
                       cub kafka-ready -b broker:29092 1 40 && \
                       echo Waiting for Confluent Schema Registry to be ready... && \
                       cub sr-ready schema-registry 8081 40 && \
                       echo Waiting a few seconds for topic creation to finish... && \
                       sleep 11 && \
                       tail -f /dev/null'"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      STREAMS_BOOTSTRAP_SERVERS: broker:29092
      STREAMS_SCHEMA_REGISTRY_HOST: schema-registry
      STREAMS_SCHEMA_REGISTRY_PORT: 8081
    networks:
      - camunda-platform

  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.2.2
    depends_on:
      - broker
      - schema-registry
    ports:
      - 8082:8082
    hostname: rest-proxy
    container_name: rest-proxy
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: 'broker:29092'
      KAFKA_REST_LISTENERS: "http://0.0.0.0:8082"
      KAFKA_REST_SCHEMA_REGISTRY_URL: 'http://schema-registry:8081'
    networks:
      - camunda-platform

volumes:
  zeebe:
  elastic:

networks:
  camunda-platform:
```
