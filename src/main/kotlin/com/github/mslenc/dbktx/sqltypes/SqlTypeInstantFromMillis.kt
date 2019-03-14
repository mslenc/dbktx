package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLong
import com.github.mslenc.dbktx.util.Sql

import java.time.Instant
import kotlin.reflect.KClass

class SqlTypeInstantFromMillis(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Instant>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.BIGINT)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Instant {
        return Instant.ofEpochMilli(value.asLong())
    }

    override fun makeDbValue(value: Instant): DbValue {
        return DbValueLong(value.toEpochMilli())
    }

    override fun encodeForJson(value: Instant): Long {
        return value.toEpochMilli()
    }

    override fun decodeFromJson(value: Any): Instant {
        if (value is Number)
            return Instant.ofEpochMilli(value.toLong())

        throw IllegalArgumentException("Not a number: $value")
    }

    override val zeroValue: Instant = Instant.ofEpochMilli(0L)

    override fun toSql(value: Instant, sql: Sql) {
        sql(value.toEpochMilli())
    }

    override val kotlinType: KClass<Instant> = Instant::class
}