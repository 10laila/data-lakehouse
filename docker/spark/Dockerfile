FROM bitnamilegacy/spark:3.5 

USER root

RUN mkdir -p /opt/bitnami/spark/jars && cd /opt/bitnami/spark/jars && \
    curl -L -o iceberg-spark-runtime.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -L -o iceberg-aws-bundle.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar && \
    curl -L -o hadoop-aws.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/app