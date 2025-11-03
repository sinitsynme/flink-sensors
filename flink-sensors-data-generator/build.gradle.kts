plugins {
    id("java")
}

group = "ru.sinitsynme"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val kafkaClientsVersion: String by project
val junitVersion: String by project
val jacksonVersion: String by project

dependencies {
    implementation(project(":sensors-commons"))
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}