package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueYear
import com.github.mslenc.dbktx.util.Sql

import java.time.Year
import kotlin.reflect.KClass

class SqlTypeYear(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Year>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.YEAR)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Year {
        return value.asYear()
    }

    override fun makeDbValue(value: Year): DbValue {
        return DbValueYear(value)
    }

    override fun encodeForJson(value: Year): Any {
        return value.value
    }

    override fun decodeFromJson(value: Any): Year {
        if (value is Number)
            return Year.of(value.toInt())

        throw IllegalArgumentException("Not a year value - $value")
    }

    override val zeroValue: Year = Year.of(0) // 1BC ?

    override fun toSql(value: Year, sql: Sql) {
        sql(value.value)
    }

    override val kotlinType: KClass<Year> = Year::class
}