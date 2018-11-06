package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueDuration
import com.github.mslenc.dbktx.util.Sql
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import kotlin.reflect.KClass

class SqlTypeLocalTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalTime>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIME)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): LocalTime {
        return value.asLocalTime()
    }

    override fun makeDbValue(value: LocalTime): DbValue {
        return DbValueDuration(Duration.ofSeconds(value.toSecondOfDay().toLong(), value.nano.toLong()))
    }

    override fun decodeFromJson(value: Any): LocalTime {
        if (value is CharSequence)
            return LocalTime.parse(value.toString(), LOCAL_TIME_FORMAT)

        throw IllegalArgumentException("Not a string(time) value - $value")
    }

    override fun encodeForJson(value: LocalTime): Any {
        return value.format(LOCAL_TIME_FORMAT)
    }

    override val dummyValue: LocalTime = LocalTime.now()

    override fun toSql(value: LocalTime, sql: Sql) {
        sql(value)
    }

    override val kotlinType: KClass<LocalTime> = LocalTime::class

    companion object {
        // the default format skips seconds, if they're zero, but we don't want that

        val LOCAL_TIME_FORMAT = DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .toFormatter()
    }
}