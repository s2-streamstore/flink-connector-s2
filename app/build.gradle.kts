group = "org.example"

plugins {
    application
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
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

val flinkVersion = "1.20.0"
val grpcVersion = "1.64.0"
var log4jVersion = "2.17.1"
var s2Version = "0.0.13"

dependencies {
    implementation(project(":lib"))
    implementation("com.google.guava:guava:33.4.0-jre")
    compileOnly("com.amazonaws:aws-kinesisanalytics-runtime:1.2.0")
    implementation("dev.s2:s2-sdk:$s2Version")
    implementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    implementation("org.apache.flink:flink-table-planner-loader:$flinkVersion")
    implementation("org.apache.flink:flink-connector-files:$flinkVersion")
    implementation("org.apache.flink:flink-json:$flinkVersion")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    implementation("com.google.protobuf:protobuf-java:4.29.3")
    implementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
}

tasks.test {
    useJUnitPlatform()
}

val executables = listOf(
    "org.example.app.eventstream.EventSpoofer",
    "org.example.app.eventstream.EventStreamJob",
    "org.example.app.eventstream.EventStreamWithContextJob",
)

executables.forEach { mainClassName ->
    val name = mainClassName.substringAfterLast('.')
    tasks.register<JavaExec>("run$name") {
        group = "application"
        description = "Run the $name demo app."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set(mainClassName)
        jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED")
    }
}
java {
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

application {
    mainClass.set("org.example.app.eventstream.EventStreamJob")
}

tasks.shadowJar {
    archiveBaseName.set("testApp")
    archiveVersion.set("")
    archiveClassifier.set("")

    dependencies {
        exclude(dependency("org.apache.flink:force-shading:"))
        exclude(dependency("com.google.code.findbugs:jsr305:"))
        exclude(dependency("org.slf4j:"))
        exclude(dependency("log4j:"))
    }
    mergeServiceFiles()

}
