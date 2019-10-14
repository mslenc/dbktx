package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException

class ExprCoalesce<E, T: Any> private constructor (private val options: List<Expr<E, T>>) : Expr<E, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("COALESCE(")
        sql.tuple(options) {
            it.toSql(sql, true)
        }
        sql.raw(")")
    }

    override val couldBeNull: Boolean
        get() {
            for (option in options)
                if (!option.couldBeNull)
                    return false
            return true
        }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return ExprCoalesce(options.map { it.remap(remapper) })
    }

    override fun getSqlType(): SqlType<T> {
        return options[0].getSqlType()
    }

    companion object {
        fun <E, T: Any> create(options: List<Expr<E, T>>, ifAllNull: T? = null): Expr<E, T> {
            if (options.isEmpty())
                throw IllegalArgumentException("There must be at least one option for coalesce")

            if (options.size == 1 && ifAllNull == null)
                return options[0]

            return if (ifAllNull != null) {
                val sqlType = options[0].getSqlType()
                val literal: Expr<E, T> = Literal(ifAllNull, sqlType)
                val combinedOptions = options + literal
                ExprCoalesce(combinedOptions)
            } else {
                ExprCoalesce(options)
            }
        }
    }
}