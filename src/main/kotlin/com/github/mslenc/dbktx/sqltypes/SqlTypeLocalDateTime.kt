package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLocalDateTime
import com.github.mslenc.dbktx.util.Sql

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import kotlin.reflect.KClass

class SqlTypeLocalDateTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalDateTime>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.DATETIME)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): LocalDateTime {
        return value.asLocalDateTime()
    }

    override fun makeDbValue(value: LocalDateTime): DbValue {
        return DbValueLocalDateTime(value)
    }

    override fun encodeForJson(value: LocalDateTime): Any {
        return value.format(LOCAL_DATE_TIME_FORMAT)
    }

    override fun decodeFromJson(value: Any): LocalDateTime {
        if (value is CharSequence)
            return LocalDateTime.parse(value, LOCAL_DATE_TIME_FORMAT)

        throw IllegalArgumentException("Not a valid string(datetime) value - $value")
    }

    override fun toSql(value: LocalDateTime, sql: Sql) {
        sql(value)
    }

    override val zeroValue: LocalDateTime = LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC)

    override val kotlinType: KClass<LocalDateTime> = LocalDateTime::class

    companion object {
        // the default format skips seconds, if they're zero, but we don't want that

        val LOCAL_DATE_TIME_FORMAT: DateTimeFormatter = DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral('T')
                .append(SqlTypeLocalTime.LOCAL_TIME_FORMAT)
                .toFormatter()
    }
}