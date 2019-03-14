package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueInt
import com.github.mslenc.dbktx.util.Sql
import kotlin.reflect.KClass

class SqlTypeInt(concreteType: SqlTypeKind, isNotNull: Boolean, isAutoGenerated: Boolean, isUnsigned: Boolean) : SqlTypeNumeric<Int>(isNotNull, isAutoGenerated, isUnsigned) {
    private val minVal: Int
    private val maxVal: Int

    init {
        when (concreteType) {
            SqlTypeKind.TINYINT -> if (isUnsigned) {
                this.minVal = 0
                this.maxVal = 255
            } else {
                this.minVal = java.lang.Byte.MIN_VALUE.toInt()
                this.maxVal = java.lang.Byte.MAX_VALUE.toInt()
            }

            SqlTypeKind.SMALLINT -> if (isUnsigned) {
                this.minVal = 0
                this.maxVal = 65535
            } else {
                this.minVal = java.lang.Short.MIN_VALUE.toInt()
                this.maxVal = java.lang.Short.MAX_VALUE.toInt()
            }

            SqlTypeKind.MEDIUMINT -> if (isUnsigned) {
                this.minVal = 0
                this.maxVal = 16777215
            } else {
                this.minVal = -8388608
                this.maxVal = 8388607
            }

            SqlTypeKind.INT -> {
                if (isUnsigned) {
                    throw IllegalArgumentException("INT UNSIGNED must be a Long or ULong")
                }

                this.minVal = Integer.MIN_VALUE
                this.maxVal = Integer.MAX_VALUE
            }

            else -> throw IllegalArgumentException("Unsupported type $concreteType")
        }
    }

    override fun parseDbValue(value: DbValue): Int {
        return value.asInt()
    }

    override fun makeDbValue(value: Int): DbValue {
        return DbValueInt(value)
    }

    override fun encodeForJson(value: Int): Any {
        return value
    }

    override fun decodeFromJson(value: Any): Int {
        if (value is Number)
            return value.toInt()

        throw IllegalArgumentException("Not a number: $value")
    }

    override fun toSql(value: Int, sql: Sql) {
        sql(value)
    }

    override val zeroValue: Int = 0

    override val kotlinType: KClass<Int> = Int::class
}
