plugins {
    id("java")
}

group = "ru.hse.drip.systems.distributed"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.21.0"))
    implementation("com.fasterxml.jackson.core:jackson-databind")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("runSelfTest") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i2.Main")
    args("test")
    jvmArgs("-ea")
}

tasks.register<JavaExec>("runNode1") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i2.Main")
    args("127.0.0.1", "9001", "leader")
}

tasks.register<JavaExec>("runNode2") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i2.Main")
    args("127.0.0.1", "9002", "follower")
}

tasks.register<JavaExec>("runNode3") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i2.Main")
    args("127.0.0.1", "9003", "follower")
}

tasks.register<JavaExec>("runCli") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i2.cli.CliMain")
    standardInput = System.`in`
}

tasks.register<JavaExec>("runI3SelfTest") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("test")
    jvmArgs("-ea")
}

tasks.register<JavaExec>("runI3Node1") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9101", "leader")
}

tasks.register<JavaExec>("runI3Node2") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9102", "follower")
}

tasks.register<JavaExec>("runI3Node3") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9103", "follower")
}

tasks.register<JavaExec>("runI3Node4") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9104", "follower")
}

tasks.register<JavaExec>("runI3Node5") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9105", "follower")
}

tasks.register<JavaExec>("runI3Node6") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9106", "follower")
}

tasks.register<JavaExec>("runI3Node7") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9107", "follower")
}

tasks.register<JavaExec>("runI3Node8") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9108", "follower")
}

tasks.register<JavaExec>("runI3Node9") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9109", "follower")
}

tasks.register<JavaExec>("runI3Node10") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.Main")
    args("127.0.0.1", "9110", "follower")
}

tasks.register<JavaExec>("runI3Cli") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i3.cli.CliMain")
    standardInput = System.`in`
}

tasks.register("printRuntimeClasspath") {
    doLast {
        println(sourceSets["main"].runtimeClasspath.asPath)
    }
}
