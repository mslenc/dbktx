package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.SqlBuilder
import kotlin.reflect.KClass

class SqlTypeFloat(concreteType: SqlTypeKind,
                   isNotNull: Boolean,
                   isUnsigned: Boolean) : SqlTypeNumeric<Float>(isNotNull = isNotNull, isAutoGenerated = false, isUnsigned = isUnsigned) {
    init {
        if (concreteType != SqlTypeKind.FLOAT)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): Float {
        if (value is Float)
            return value

        if (value is Number)
            return value.toFloat()

        throw IllegalArgumentException("Not a float - " + value)
    }

    override fun toJson(value: Float): Any {
        return value
    }

    override fun dummyValue(): Float {
        return Math.PI.toFloat()
    }

    override fun toSql(value: Float, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<Float> = Float::class
}