package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueOffsetDateTime
import com.github.mslenc.dbktx.util.Sql

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.reflect.KClass

class SqlTypeInstantFromOffsetTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Instant>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIMESTAMP_TZ)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Instant {
        return value.asOffsetDateTime().toInstant()
    }

    override fun makeDbValue(value: Instant): DbValue {
        return DbValueOffsetDateTime(OffsetDateTime.ofInstant(value, ZoneOffset.UTC))
    }

    override fun encodeForJson(value: Instant): String {
        return value.toString()
    }

    override fun decodeFromJson(value: Any): Instant {
        if (value is String)
            return Instant.parse(value)

        throw IllegalArgumentException("Not a timestamp: $value")
    }

    override val zeroValue: Instant = Instant.ofEpochMilli(0L)

    override fun toSql(value: Instant, sql: Sql) {
        sql(value)
    }

    override val kotlinType: KClass<Instant> = Instant::class
}