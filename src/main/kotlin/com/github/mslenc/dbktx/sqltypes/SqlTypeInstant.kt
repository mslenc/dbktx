package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueInstant
import com.github.mslenc.dbktx.util.Sql

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.reflect.KClass

class SqlTypeInstant(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Instant>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIMESTAMP)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Instant {
        return value.asInstant()
    }

    override fun makeDbValue(value: Instant): DbValue {
        return DbValueInstant(value, LocalDateTime.ofInstant(value, ZoneOffset.UTC))
    }

    override fun encodeForJson(value: Instant): Any {
        return value.toString()
    }

    override fun decodeFromJson(value: Any): Instant {
        if (value is CharSequence)
            return Instant.parse(value)

        throw IllegalArgumentException("Not a string(instant) value - $value")
    }

    override val dummyValue: Instant
        get() = Instant.now()

    override fun toSql(value: Instant, sql: Sql) {
        sql(value)
    }

    override val kotlinType: KClass<Instant> = Instant::class
}