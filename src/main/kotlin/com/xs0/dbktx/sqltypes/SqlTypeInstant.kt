package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.SqlBuilder

import java.time.Instant
import kotlin.reflect.KClass

class SqlTypeInstant(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Instant>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIMESTAMP)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): Instant {
        if (value is CharSequence)
            return Instant.parse(value)

        throw IllegalArgumentException("Not a string(instant) value - " + value)
    }

    override fun toJson(value: Instant): String {
        return value.toString()
    }

    override fun dummyValue(): Instant {
        return Instant.now()
    }

    override fun toSql(value: Instant, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<Instant> = Instant::class
}