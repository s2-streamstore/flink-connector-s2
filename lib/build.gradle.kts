plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    id("maven-publish")
}

group = "dev.s2"
version = project.rootProject.property("version") as String

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

dependencies {
    compileOnly(libs.flink.connector.base)
    compileOnly(libs.flink.java)
    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.table.api.java.bridge)
    compileOnly(libs.protobuf.java)
    implementation(libs.guava)
    implementation(libs.s2.internal)
    implementation(libs.s2.sdk)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
    withJavadocJar()
    withSourcesJar()
}

var protobufVersion = "4.29.3"
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "flink-connector-s2"
            pom {
                name.set("Flink connector for S2.")
                description.set("Apache Flink connector code for interacting with S2 streams.")
                url.set("https://github.com/s2-streamstore/flink-connector-s2")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    connection = "scm:git:git@github.com:s2-streamstore/flink-connector-s2.git"
                    url = "https://github.com/s2-streamstore/flink-connector-s2"
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/s2-streamstore/flink-connector-s2")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

}