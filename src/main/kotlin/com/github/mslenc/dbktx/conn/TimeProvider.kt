package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.DbConnection
import java.time.*

interface TimeProvider {
    suspend fun getTime(conn: DbConnection): RequestTime
}

interface RequestTime {
    val instant: Instant
    val zonedDateTime: ZonedDateTime
    val localDateTime: LocalDateTime

    val localDate: LocalDate get() = localDateTime.toLocalDate()
    val localTime: LocalTime get() = localDateTime.toLocalTime()

    val timeZone: ZoneId

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

    override val timeZone: ZoneId
        get() = zonedDateTime.zone
}

class TimeProviderFromClock(val clock: Clock): TimeProvider {
    override suspend fun getTime(conn: DbConnection): RequestTime {
        return getTime()
    }

    fun getTime(): RequestTime {
        return SimpleRequestTime(clock.instant(), clock.zone)
    }
}