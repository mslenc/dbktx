package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLong
import com.github.mslenc.dbktx.util.Sql
import kotlin.reflect.KClass

class SqlTypeLong(
        private val concreteType: SqlTypeKind,
        isNotNull: Boolean,
        isAutoGenerated: Boolean,
        isUnsigned: Boolean)

    : SqlTypeNumeric<Long>(isNotNull = isNotNull,
                           isAutoGenerated = isAutoGenerated,
                           isUnsigned = isUnsigned) {

    init {
        when (concreteType) {
            SqlTypeKind.INT -> {
                // ok
            }

            SqlTypeKind.BIGINT -> if (isUnsigned) {
                throw IllegalArgumentException("Use ULong for unsigned bigint")
            }

            else ->
                throw IllegalArgumentException("Unsupported type $concreteType")
        }
    }

    override fun parseDbValue(value: DbValue): Long {
        return value.asLong()
    }

    override fun makeDbValue(value: Long): DbValue {
        return DbValueLong(value)
    }

    override fun encodeForJson(value: Long): Any {
        return value
    }

    override fun decodeFromJson(value: Any): Long {
        if (value is Number)
            return value.toLong()

        throw IllegalArgumentException("Not a number: $value")
    }

    override fun toSql(value: Long, sql: Sql) {
        sql(value)
    }

    override val zeroValue: Long = 0L

    override val kotlinType: KClass<Long> = Long::class

    companion object {
        val INSTANCE_FOR_COUNT = SqlTypeLong(SqlTypeKind.BIGINT, true, false, false)
    }
}