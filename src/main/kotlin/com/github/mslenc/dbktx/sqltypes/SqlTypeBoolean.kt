package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueBoolean
import com.github.mslenc.dbktx.util.Sql

import kotlin.reflect.KClass

class SqlTypeBoolean(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Boolean>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.BIT && concreteType != SqlTypeKind.BOOLEAN)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Boolean {
        return value.asBoolean()
    }

    override fun makeDbValue(value: Boolean): DbValue {
        return DbValueBoolean.of(value)
    }

    override fun encodeForJson(value: Boolean): Any {
        return value
    }

    override fun decodeFromJson(value: Any): Boolean {
        if (value is Boolean)
            return value

        throw IllegalArgumentException("Not a boolean value - $value")
    }

    override val zeroValue: Boolean = false

    override fun toSql(value: Boolean, sql: Sql) {
        sql.raw(if (value) "TRUE" else "FALSE")
    }

    override val kotlinType: KClass<Boolean> = Boolean::class

    companion object {
        val INSTANCE_FOR_FILTER = SqlTypeBoolean(SqlTypeKind.BOOLEAN, true)
    }
}