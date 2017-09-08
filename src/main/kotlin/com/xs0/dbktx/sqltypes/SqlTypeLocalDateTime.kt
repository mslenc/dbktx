package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.SqlBuilder

import java.time.LocalDateTime
import kotlin.reflect.KClass

class SqlTypeLocalDateTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalDateTime>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.DATETIME)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): LocalDateTime {
        if (value is CharSequence)
            return LocalDateTime.parse(value)

        throw IllegalArgumentException("Not a valid string(datetime) value - " + value)
    }

    override fun toJson(value: LocalDateTime): String {
        return value.toString()
    }

    override fun dummyValue(): LocalDateTime {
        return LocalDateTime.now()
    }

    override fun toSql(value: LocalDateTime, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value.toString())
    }

    override val kotlinType: KClass<LocalDateTime> = LocalDateTime::class
}