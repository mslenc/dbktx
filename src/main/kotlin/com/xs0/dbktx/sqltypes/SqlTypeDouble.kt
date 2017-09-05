package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

class SqlTypeDouble(concreteType: SqlTypeKind, fieldProps: FieldProps) : SqlTypeNumeric<Double>(fieldProps) {
    private val minVal: Double
    private val maxVal: Double

    init {
        when (concreteType) {
            SqlTypeKind.FLOAT -> {
                this.minVal = if (isUnsigned) 0.0 else -Float.MAX_VALUE.toDouble()
                this.maxVal = Float.MAX_VALUE.toDouble()
            }

            SqlTypeKind.DOUBLE -> {
                this.minVal = if (isUnsigned) 0.0 else -Double.MAX_VALUE
                this.maxVal = Double.MAX_VALUE
            }

            else -> throw IllegalArgumentException("Unsupported type " + concreteType)
        }
    }

    override fun fromJson(value: Any): Double {
        if (value is Double)
            return value

        if (value is Number)
            return value.toDouble()

        throw IllegalArgumentException("Not a double - " + value)
    }

    override fun toJson(value: Double): Any {
        return value
    }

    override fun dummyValue(): Double {
        return 2 * Math.PI
    }

    override fun toSql(value: Double, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<Double> = Double::class
}
