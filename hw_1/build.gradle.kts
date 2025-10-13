plugins {
    id("java")
    id("application")
}

group = "ru.hse.drip.systems.distributed"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("ru.hse.drip.systems.distributed.seminar.i3.balancer.SimpleLoadBalancer")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "ru.hse.drip.systems.distributed.seminar.i3.balancer.SimpleLoadBalancer"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}