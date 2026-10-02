plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spennende.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)

    implementation(libs.postgresql)
    implementation(libs.hikariCP)
    implementation(libs.kotliquery)
    implementation(libs.flyway.database.postgresql)

    api(libs.tbdLibs.azureTokenClientDefault)
    api(libs.tbdLibs.retry)
    api(libs.tbdLibs.speedClient)

    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
    testImplementation(libs.mockk)
    testImplementation(libs.tbdLibs.postgresTestdatabaser)
}
