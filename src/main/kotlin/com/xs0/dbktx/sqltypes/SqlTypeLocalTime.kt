package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import java.time.LocalTime
import kotlin.reflect.KClass

class SqlTypeLocalTime(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<LocalTime>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIME)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): LocalTime {
        if (value is CharSequence)
            return LocalTime.parse(value.toString())

        throw IllegalArgumentException("Not a string(time) value - " + value)
    }

    override fun toJson(value: LocalTime): String {
        return value.toString()
    }

    override val dummyValue: LocalTime = LocalTime.now()

    override fun toSql(value: LocalTime, sql: Sql) {
        sql(value)
    }

    override val kotlinType: KClass<LocalTime> = LocalTime::class
}