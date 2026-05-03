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

tasks.register<JavaExec>("runI4SelfTest") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("test")
    jvmArgs("-ea")
}

tasks.register<JavaExec>("runI4Node1") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9101", "leader")
}

tasks.register<JavaExec>("runI4Node2") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9102", "follower")
}

tasks.register<JavaExec>("runI4Node3") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9103", "follower")
}

tasks.register<JavaExec>("runI4Node4") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9104", "follower")
}

tasks.register<JavaExec>("runI4Node5") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9105", "follower")
}

tasks.register<JavaExec>("runI4Node6") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9106", "follower")
}

tasks.register<JavaExec>("runI4Node7") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9107", "follower")
}

tasks.register<JavaExec>("runI4Node8") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9108", "follower")
}

tasks.register<JavaExec>("runI4Node9") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9109", "follower")
}

tasks.register<JavaExec>("runI4Node10") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.Main")
    args("127.0.0.1", "9110", "follower")
}

tasks.register<JavaExec>("runI4Cli") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.cli.CliMain")
    standardInput = System.`in`
}

tasks.register<JavaExec>("runI4RequiredBenchmarks") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i4.benchmarks.BenchmarkMain")
    args(
        "--nodes", "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105,127.0.0.1:9106,127.0.0.1:9107",
        "--output", "src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/results.csv",
        "--report", "src/main/java/ru/hse/drip/systems/distributed/homework/i4/report.md"
    )
}

tasks.register<Exec>("runI4RequiredBenchmarkSuite") {
    dependsOn("classes")
    workingDir = projectDir
    commandLine(
        "bash",
        "src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/run-required-benchmarks.sh"
    )
}

tasks.register<JavaExec>("runI5SelfTest") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("test")
    jvmArgs("-ea")
}

tasks.register<JavaExec>("runI5Router") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("router", "127.0.0.1", "9200")
}

tasks.register<JavaExec>("runI5Node1") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("node", "A", "127.0.0.1", "9201")
}

tasks.register<JavaExec>("runI5Node2") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("node", "B", "127.0.0.1", "9202")
}

tasks.register<JavaExec>("runI5Node3") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("node", "C", "127.0.0.1", "9203")
}

tasks.register<JavaExec>("runI5Node4") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("node", "D", "127.0.0.1", "9204")
}

tasks.register<JavaExec>("runI5Node5") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("node", "E", "127.0.0.1", "9205")
}

tasks.register<JavaExec>("runI5Cli") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args("cli", "127.0.0.1", "9200")
    standardInput = System.`in`
}

tasks.register<JavaExec>("runI5LoadKeys") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.tools.LoadKeysMain")
}

tasks.register<JavaExec>("runI5RequiredBenchmarks") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("ru.hse.drip.systems.distributed.homework.i5.Main")
    args(
        "benchmark",
        "--output", "src/main/java/ru/hse/drip/systems/distributed/homework/i5/benchmarks/results.csv"
    )
}

tasks.register("printRuntimeClasspath") {
    doLast {
        println(sourceSets["main"].runtimeClasspath.asPath)
    }
}
