plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

val flinkVersion: String by project
val kafkaClientsVersion: String by project
val flinkConnectorKafkaVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":sensors-commons"))
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:$flinkConnectorKafkaVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
}

tasks {
    named<JavaCompile>("compileJava") {
        options.release.set(17)
    }

    // Shadow Jar task configuration
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        manifest {
            attributes("Main-Class" to "com.learn.flink.WordCountJob")
        }

        exclude("META-INF/*.SF")
        exclude("META-INF/*.DSA")
        exclude("META-INF/*.RSA")

        mergeServiceFiles()
    }

    // Make build depend on shadowJar
    named("build") {
        dependsOn("shadowJar")
    }
}