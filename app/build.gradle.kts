group = "org.example"

plugins {
    application
    java
}

repositories {
    google()
    mavenCentral()
    mavenLocal()
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/s2-streamstore/s2-sdk-java")
        credentials {
            username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.token") as String? ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

val flinkVersion = "1.19.1"
val grpcVersion = "1.64.0"
var log4jVersion = "2.17.1"
var s2Version = "0.0.12"

dependencies {
    implementation(project(":lib"))
    implementation("dev.s2:s2-sdk:$s2Version")
    implementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    implementation("org.apache.flink:flink-table-planner-loader:$flinkVersion")
    implementation("org.apache.flink:flink-connector-files:$flinkVersion")
    implementation("org.apache.flink:flink-json:$flinkVersion")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
}

tasks.test {
    useJUnitPlatform()
}

java {
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}
