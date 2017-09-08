package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.SqlBuilder

import java.time.LocalDate
import kotlin.reflect.KClass

class SqlTypeLocalDate(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalDate>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.DATE)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): LocalDate {
        if (value is CharSequence)
            return LocalDate.parse(value)

        throw IllegalArgumentException("Not a string(date) value - " + value)
    }

    override fun toJson(value: LocalDate): String {
        return value.toString()
    }

    override fun dummyValue(): LocalDate {
        return LocalDate.now()
    }

    override fun toSql(value: LocalDate, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value.toString())
    }

    override val kotlinType: KClass<LocalDate> = LocalDate::class
}