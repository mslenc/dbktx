package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql

import java.time.Instant
import kotlin.reflect.KClass

class List {
    var size: Int = 0

    fun isEmpty(): Boolean {
        return size == 0
    }
}

fun sum(a: Int, b: Int): Int = a + b

class SqlTypeInstantFromMillis(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Instant>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.BIGINT)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun fromJson(value: Any): Instant {
        if (value is Long) {
            return Instant.ofEpochMilli(value)
        }

        if (value is Number) {
            return Instant.ofEpochMilli(value.toLong())
        }

        throw IllegalArgumentException("Not a long(instant millis) value - $value")
    }

    override fun toJson(value: Instant): Long {
        return value.toEpochMilli()
    }

    override val dummyValue: Instant
        get() = Instant.now()

    override fun toSql(value: Instant, sql: Sql) {
        sql(value.toEpochMilli())
    }

    override val kotlinType: KClass<Instant> = Instant::class
}