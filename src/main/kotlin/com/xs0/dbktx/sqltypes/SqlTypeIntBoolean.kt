package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.SqlBuilder
import kotlin.reflect.KClass

class SqlTypeIntBoolean(concreteType: SqlTypeKind, isNotNull: Boolean, trueValue: Int?, falseValue: Int?) : SqlType<Boolean>(isNotNull = isNotNull) {
    private val trueValue: Int // true will convert to this
    private val falseValue: Int // false will convert to this
    private val checkNonFalse: Boolean // if true, we check that dbValue != falseValue, otherwise we check that dbValue == trueValue

    init {
        if (trueValue != null) {
            if (falseValue != null) {
                if (trueValue == falseValue)
                    throw IllegalArgumentException("trueValue and falseValue must be different")

                // both specified: we take anything not falseValue to be true, and use trueValue just for encoding back
                this.trueValue = trueValue
                this.falseValue = falseValue
                this.checkNonFalse = true
            } else {
                // only true specified: we take only trueValue as true, and the rest as false
                this.trueValue = trueValue
                this.falseValue = if (trueValue == 0) 1 else 0 // default to 0, except if trueValue is already 0
                this.checkNonFalse = false
            }
        } else {
            if (falseValue != null) {
                // only false specified: we take only falseValue as false, and the rest as true
                this.falseValue = falseValue
                this.trueValue = if (falseValue == 1) 0 else 1 // default to 1, except if falseValue is already 1
                this.checkNonFalse = true
            } else {
                throw IllegalArgumentException("At least one of trueValue and falseValue must be specified")
            }
        }

        when (concreteType) {
            SqlTypeKind.TINYINT, SqlTypeKind.SMALLINT, SqlTypeKind.MEDIUMINT, SqlTypeKind.INT -> {
                // ok
            }

            else ->
                throw IllegalArgumentException("Unsupported type " + concreteType)
        }
    }

    override fun fromJson(value: Any): Boolean {
        val intVal: Int?
        when (value) {
            is Int -> intVal = value
            is Number -> intVal = value.toInt()
            else -> throw IllegalArgumentException("Not an integer(bool) - " + value)
        }

        return if (checkNonFalse) {
            falseValue != intVal
        } else {
            trueValue == intVal
        }
    }

    override fun toJson(value: Boolean): Int {
        return if (value) trueValue else falseValue
    }

    override fun dummyValue(): Boolean {
        return java.lang.Boolean.TRUE
    }

    override fun toSql(value: Boolean, sb: SqlBuilder, topLevel: Boolean) {
        val rep: Int = if (value) trueValue else falseValue

        sb.sql("?").param(rep)
    }

    override val kotlinType: KClass<Boolean> = Boolean::class
}
