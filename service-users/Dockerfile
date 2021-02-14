FROM openjdk:15-jdk-alpine3.12

RUN mkdir app
COPY target/*.jar /app/app.jar 
RUN cd app
CMD ["java","-jar","/app.jar"]