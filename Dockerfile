FROM openjdk:11-slim

COPY target/bakeoff-akka-http.jar .
CMD java -jar bakeoff-akka-http.jar