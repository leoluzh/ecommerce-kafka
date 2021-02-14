FROM openjdk:15-jdk-alpine3.12

RUN adduser -D javauser
USER javauser

RUN mkdir -p /app
WORKDIR /app

COPY /target/*.jar /app/runapp.jar

CMD ["java","-jar","/runapp.jar"]