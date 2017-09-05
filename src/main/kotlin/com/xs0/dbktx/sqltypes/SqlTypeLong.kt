package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

class SqlTypeLong(private val concreteType: SqlTypeKind, fieldProps: FieldProps) : SqlTypeNumeric<Long>(fieldProps) {
    private val minVal: Long
    private val maxVal: Long

    init {

        when (concreteType) {
            SqlTypeKind.INT -> if (isUnsigned) {
                this.minVal = 0
                this.maxVal = 4_294_967_295L
            } else {
                this.minVal = Integer.MIN_VALUE.toLong()
                this.maxVal = Integer.MAX_VALUE.toLong()
            }

            SqlTypeKind.BIGINT -> {
                this.minVal = java.lang.Long.MIN_VALUE
                this.maxVal = java.lang.Long.MAX_VALUE
            }

            else -> throw IllegalArgumentException("Unsupported type " + concreteType)
        }
    }

    fun equal(left: Long, right: Long): Boolean {
        return left == right
    }

    fun compare(left: Long, right: Long): Int {
        return left.compareTo(right)
    }

    override fun fromJson(value: Any): Long {
        if (value is Long)
            return value

        if (value is Number)
            return value.toLong()

        throw IllegalArgumentException("Not a long - " + value)
    }

    override fun toJson(value: Long): Any {
        return value
    }

    override fun dummyValue(): Long {
        return maxVal / 2
    }

    override fun toSql(value: Long, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?")
        if (isUnsigned && concreteType === SqlTypeKind.BIGINT && value < 0) {
            sb.param(java.lang.Long.toUnsignedString(value))
        } else {
            sb.param(value)
        }
    }

    override val kotlinType: KClass<Long> = Long::class
}