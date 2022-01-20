FROM openjdk:11-jre

COPY target/flink-operator-1.0-SNAPSHOT.jar /

CMD ["java", "-jar", "/flink-operator-1.0-SNAPSHOT.jar"]
