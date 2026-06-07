# Stage 1: Build JAR inside Docker (no local Maven required on server)
FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build
COPY pom.xml .
# Download dependencies first (cached layer)
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q

# Stage 2: Lean runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/target/payroll-1.0.0.jar app.jar

EXPOSE 8081

CMD ["java", "-Xmx512m", "-Xms256m", \
     "-XX:+UseContainerSupport", \
     "-jar", "app.jar"]
