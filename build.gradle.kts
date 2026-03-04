plugins {
    kotlin("jvm") version "2.3.0"
}

// Sett opp repositories basert på om vi kjører i CI eller ikke
// Jf. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
repositories {
    mavenCentral()
    if (providers.environmentVariable("GITHUB_ACTIONS").orNull == "true") {
        maven {
            url = uri("https://maven.pkg.github.com/navikt/maven-release")
            credentials {
                username = "token"
                password = providers.environmentVariable("GITHUB_TOKEN").orNull!!
            }
        }
    } else {
        maven("https://repo.adeo.no/repository/github-package-registry-navikt/")
    }
}

val flywayVersion = "11.5.0"
val hikariCPVersion = "6.3.0"
val kotliqueryVersion = "1.9.0"
val junitJupiterVersion = "5.12.1"
val tbdLibsVersion = "2026.01.22-09.16-1d3f6039"
val mockkVersion = "1.13.17"
val rapidsAndRiversVersion = "2026011411051768385145.e8ebad1177b4"
val postgresqlVersion = "42.7.7"

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("com.github.seratch:kotliquery:$kotliqueryVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")

    api("com.github.navikt.tbd-libs:azure-token-client-default:$tbdLibsVersion")
    api("com.github.navikt.tbd-libs:retry:$tbdLibsVersion")
    api("com.github.navikt.tbd-libs:speed-client:$tbdLibsVersion")

    testImplementation("com.github.navikt.tbd-libs:rapids-and-rivers-test:$tbdLibsVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("com.github.navikt.tbd-libs:postgres-testdatabaser:$tbdLibsVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}


kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("21"))
    }
}

tasks {

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
    named<Jar>("jar") {
        archiveFileName.set("app.jar")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.spennende.AppKt"
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists()) it.copyTo(file)
            }
        }
    }
}
