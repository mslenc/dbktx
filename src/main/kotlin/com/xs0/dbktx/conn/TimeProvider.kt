package com.xs0.dbktx.conn

import io.vertx.ext.sql.SQLConnection
import java.time.*

interface TimeProvider {
    suspend fun getTime(conn: SQLConnection): RequestTime
}

interface RequestTime {
    val instant: Instant
    val zonedDateTime: ZonedDateTime
    val localDateTime: LocalDateTime

    val localDate: LocalDate get() = localDateTime.toLocalDate()
    val localTime: LocalTime get() = localDateTime.toLocalTime()

    companion object {
        fun forTesting(): RequestTime {
            return SimpleRequestTime(Instant.now(), ZoneId.systemDefault())
        }
    }
}

class SimpleRequestTime(override val instant: Instant, zoneId: ZoneId): RequestTime {
    override val zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId)

    override val localDateTime: LocalDateTime
        get() = zonedDateTime.toLocalDateTime()
}

class TimeProviderFromClock(val clock: Clock): TimeProvider {
    override suspend fun getTime(conn: SQLConnection): RequestTime {
        return SimpleRequestTime(clock.instant(), clock.zone)
    }
}