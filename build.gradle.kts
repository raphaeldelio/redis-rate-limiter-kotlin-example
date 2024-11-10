plugins {
    kotlin("jvm") version "2.0.20"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("redis.clients:jedis:5.2.0")
    testImplementation(kotlin("test"))
    testImplementation("com.redis:testcontainers-redis:2.2.2")
    testImplementation("org.assertj:assertj-core:3.11.1")

}

tasks.test {
    useJUnitPlatform()
}