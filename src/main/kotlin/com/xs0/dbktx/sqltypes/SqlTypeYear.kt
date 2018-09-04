package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql

import java.time.Year
import kotlin.reflect.KClass

class SqlTypeYear(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Year>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.YEAR)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseRowDataValue(value: Any): Year {
        if (value is Year)
            return value

        throw IllegalArgumentException("Not a year value - $value")
    }

    override fun encodeForJson(value: Year): Any {
        return value.value
    }

    override fun decodeFromJson(value: Any): Year {
        if (value is Number)
            return Year.of(value.toInt())

        throw IllegalArgumentException("Not a year value - $value")
    }

    override val dummyValue: Year = Year.of(2017)

    override fun toSql(value: Year, sql: Sql) {
        sql(value.value)
    }

    override val kotlinType: KClass<Year> = Year::class
}