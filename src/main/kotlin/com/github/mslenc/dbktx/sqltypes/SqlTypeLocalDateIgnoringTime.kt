package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLocalDateTime
import com.github.mslenc.dbktx.util.Sql
import java.time.LocalDate

import java.time.format.DateTimeFormatter
import kotlin.reflect.KClass

class SqlTypeLocalDateIgnoringTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalDate>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.DATETIME)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): LocalDate {
        return value.asLocalDateTime().toLocalDate()
    }

    override fun makeDbValue(value: LocalDate): DbValue {
        return DbValueLocalDateTime(value.atStartOfDay())
    }

    override fun encodeForJson(value: LocalDate): Any {
        return value.format(DateTimeFormatter.ISO_LOCAL_DATE)
    }

    override fun decodeFromJson(value: Any): LocalDate {
        if (value is CharSequence)
            return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)

        throw IllegalArgumentException("Not a valid string(date) value - $value")
    }

    override fun toSql(value: LocalDate, sql: Sql) {
        sql(value)
    }

    override val zeroValue: LocalDate = LocalDate.ofEpochDay(0L)

    override val kotlinType: KClass<LocalDate> = LocalDate::class
}