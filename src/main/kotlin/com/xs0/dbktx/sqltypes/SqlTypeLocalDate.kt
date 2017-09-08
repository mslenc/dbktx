package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql

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

    override fun toSql(value: LocalDate, sql: Sql) {
        sql(value)
    }

    override val dummyValue: LocalDate
        get() = LocalDate.now()

    override val kotlinType: KClass<LocalDate> = LocalDate::class
}