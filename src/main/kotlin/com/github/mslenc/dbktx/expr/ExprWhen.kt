package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException

class ExprWhen<T: Any> private constructor (private val options: List<Pair<Expr<Boolean>, Expr<T>>>, private val elseOption: Expr<T>?) : Expr<T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("CASE")
        for (option in options) {
            sql.raw(" WHEN ")
            option.first.toSql(sql, true)
            sql.raw(" THEN ")
            option.second.toSql(sql, true)
        }
        if (elseOption != null) {
            sql.raw(" ELSE ")
            elseOption.toSql(sql, true)
        }
        sql.raw(" END")
    }

    override val couldBeNull: Boolean
        get() = when {
            options.any { it.second.couldBeNull } -> true
            elseOption == null -> true
            elseOption.couldBeNull -> true
            else -> false
        }

    override val involvesAggregation: Boolean
        get() = options.any { it.first.involvesAggregation || it.second.involvesAggregation } || elseOption?.involvesAggregation == true

    override fun remap(remapper: TableRemapper): Expr<T> {
        return ExprWhen(
            options.map { it.first.remap(remapper) to it.second.remap(remapper) },
            elseOption?.remap(remapper)
        )
    }

    override val sqlType: SqlType<T>
        get() = options.first().second.sqlType

    companion object {
        fun <T: Any> create(options: List<Pair<Expr<Boolean>, Expr<T>>>, elseOption: Expr<T>? = null): Expr<T> {
            if (options.isEmpty())
                throw IllegalArgumentException("There must be at least one option for WHEN")

            return ExprWhen(options, elseOption)
        }
    }
}