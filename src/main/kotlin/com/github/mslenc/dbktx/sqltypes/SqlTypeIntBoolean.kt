package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueInt
import com.github.mslenc.dbktx.util.Sql

import kotlin.reflect.KClass

class SqlTypeIntBoolean(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Boolean>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TINYINT && concreteType != SqlTypeKind.SMALLINT && concreteType != SqlTypeKind.MEDIUMINT && concreteType != SqlTypeKind.INT)
            throw IllegalArgumentException("Unsupported type $concreteType")
    }

    override fun parseDbValue(value: DbValue): Boolean {
        return value.asInt() != 0
    }

    override fun makeDbValue(value: Boolean): DbValue {
        return DbValueInt(if (value) 1 else 0)
    }

    override fun encodeForJson(value: Boolean): Int {
        return if (value) 1 else 0
    }

    override fun decodeFromJson(value: Any): Boolean {
        if (value is Number)
            return value.toInt() != 0

        throw IllegalArgumentException("Not a number: $value")
    }

    override val dummyValue: Boolean
        get() = true

    override fun toSql(value: Boolean, sql: Sql) {
        sql(if (value) 1 else 0)
    }

    override val kotlinType: KClass<Boolean> = Boolean::class
}