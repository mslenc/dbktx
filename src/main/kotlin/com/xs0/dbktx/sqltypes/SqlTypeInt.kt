package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

class SqlTypeInt(private val concreteType: SqlTypeKind, fieldProps: FieldProps) : SqlTypeNumeric<Int>(fieldProps) {
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
                this.minVal = Integer.MIN_VALUE
                this.maxVal = Integer.MAX_VALUE
            }

            else -> throw IllegalArgumentException("Unsupported type " + concreteType)
        }
    }

    override fun fromJson(value: Any): Int {
        if (value is Int)
            return value

        if (value is Number)
            return value.toInt()

        throw IllegalArgumentException("Not an int - " + value)
    }

    override fun toJson(value: Int): Any {
        return value
    }

    override fun dummyValue(): Int {
        return maxVal / 2
    }

    override fun toSql(value: Int, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?")
        if (isUnsigned && concreteType === SqlTypeKind.INT && value < 0) {
            sb.param(Integer.toUnsignedLong(value))
        } else {
            sb.param(value)
        }
    }

    override val kotlinType: KClass<Int> = Int::class
}
