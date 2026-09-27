FROM eclipse-temurin:21-jre
COPY build/libs/rocks-timed-queue.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
