package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.SqlBuilder

import java.time.Year
import kotlin.reflect.KClass

class SqlTypeYear(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Year>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.YEAR)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): Year {
        var year = 0

        if (value is Number)
            year = value.toInt()

        if (year == 0 && value is String) {
            try {
                year = Integer.parseInt(value.toString())
            } catch (e: NumberFormatException) {
                // carry on
            }

        }

        if (year in 1000..9999)
            return Year.of(year)

        throw IllegalArgumentException("Not a year value - " + value)
    }

    override fun toJson(value: Year): Any {
        return value.value
    }

    override fun dummyValue(): Year {
        return Year.of(2017)
    }

    override fun toSql(value: Year, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value.value)
    }

    override val kotlinType: KClass<Year> = Year::class
}