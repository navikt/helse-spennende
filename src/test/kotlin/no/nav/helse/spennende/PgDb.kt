package no.nav.helse.spennende

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers

val databaseContainer = DatabaseContainers.container("spennende", CleanupStrategy.tables("person, endringsmelding"))