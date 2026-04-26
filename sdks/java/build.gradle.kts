import com.google.protobuf.gradle.*
import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.9.6"
}

group = "io.github.gardenias"
version = providers.gradleProperty("sdkVersion").getOrElse("0.1.0")

val grpcVersion: String by project
val protobufVersion: String by project
val micrometerVersion: String by project
val slf4jVersion: String by project
val junitVersion: String by project
val junitPlatformVersion: String by project
val assertjVersion: String by project
val mockitoVersion: String by project
val testcontainersVersion: String by project

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
    withJavadocJar()
    withSourcesJar()
}

sourceSets {
    named("main") {
        proto {
            srcDir("../../crates/rr-network/proto")
            include("client.proto", "raft.proto", "admin.proto")
        }
    }

    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets["main"].output + configurations.testRuntimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

configurations {
    named("integrationTestImplementation") {
        extendsFrom(configurations.testImplementation.get())
    }
    named("integrationTestRuntimeOnly") {
        extendsFrom(configurations.testRuntimeOnly.get())
    }
}

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("io.micrometer:micrometer-core:$micrometerVersion")
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

    "integrationTestImplementation"("org.junit.jupiter:junit-jupiter:$junitVersion")
    "integrationTestRuntimeOnly"("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
    "integrationTestImplementation"("org.assertj:assertj-core:$assertjVersion")
    "integrationTestImplementation"("org.mockito:mockito-core:$mockitoVersion")
    "integrationTestImplementation"("org.testcontainers:testcontainers:$testcontainersVersion")
    "integrationTestImplementation"("org.testcontainers:junit-jupiter:$testcontainersVersion")
    "integrationTestImplementation"("io.grpc:grpc-inprocess:$grpcVersion")
    "integrationTestRuntimeOnly"("org.slf4j:slf4j-simple:$slf4jVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc") {
                    option("@generated=omit")
                }
            }
        }
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("-Xshare:off")
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests."
    group = LifecycleBasePlugin.VERIFICATION_GROUP

    val integrationTestSourceSet = sourceSets["integrationTest"]
    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
}

tasks.check {
    dependsOn(integrationTest)
}

tasks.javadoc {
    source = sourceSets["main"].allJava.matching {
        include("io/github/gardenias/rr/client/**")
    }
    classpath = sourceSets["main"].output + sourceSets["main"].compileClasspath
    val javadocOptions = options as StandardJavadocDocletOptions
    javadocOptions.encoding = "UTF-8"
    javadocOptions.addBooleanOption("Xdoclint:all,-missing", true)
}

publishing {
    publications {
        create<MavenPublication>("gpr") {
            groupId = "io.github.gardenias"
            artifactId = "rr-client"
            version = project.version.toString()
            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/gardenias/rr")
            credentials(PasswordCredentials::class) {
                username = (findProperty("gpr.user") as String?) ?: System.getenv("GITHUB_ACTOR")
                password = (findProperty("gpr.key") as String?) ?: System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
