# Use a lightweight Java 21 runtime as the base image
FROM eclipse-temurin:21-jre-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled JAR file from your target folder into the container
# (Adjust the JAR name if your actual generated JAR is named differently)
COPY target/projection-handler-0.0.1-SNAPSHOT.jar app.jar

# Expose the default Spring Boot port
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
