package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLocalDate
import com.github.mslenc.dbktx.util.Sql

import java.time.LocalDate
import kotlin.reflect.KClass

class SqlTypeLocalDate(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalDate>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.DATE)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): LocalDate {
        return value.asLocalDate()
    }

    override fun makeDbValue(value: LocalDate): DbValue {
        return DbValueLocalDate(value)
    }

    override fun encodeForJson(value: LocalDate): Any {
        return value.toString()
    }

    override fun decodeFromJson(value: Any): LocalDate {
        if (value is CharSequence)
            return LocalDate.parse(value)

        throw IllegalArgumentException("Not a string value (for LocalDate) - $value")
    }

    override fun toSql(value: LocalDate, sql: Sql) {
        sql(value)
    }

    override val dummyValue: LocalDate
        get() = LocalDate.now()

    override val kotlinType: KClass<LocalDate> = LocalDate::class
}