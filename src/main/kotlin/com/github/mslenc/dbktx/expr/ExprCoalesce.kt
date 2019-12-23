package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException

class ExprCoalesce<T: Any> private constructor (private val options: List<Expr<T>>) : Expr<T> {
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

    override fun remap(remapper: TableRemapper): Expr<T> {
        return ExprCoalesce(options.map { it.remap(remapper) })
    }

    override fun getSqlType(): SqlType<T> {
        return options[0].getSqlType()
    }

    companion object {
        fun <T: Any> create(options: List<Expr<T>>, ifAllNull: T? = null): Expr<T> {
            if (options.isEmpty())
                throw IllegalArgumentException("There must be at least one option for coalesce")

            if (options.size == 1 && ifAllNull == null)
                return options[0]

            return if (ifAllNull != null) {
                val sqlType = options[0].getSqlType()
                val literal: Expr<T> = Literal(ifAllNull, sqlType)
                val combinedOptions = options + literal
                ExprCoalesce(combinedOptions)
            } else {
                ExprCoalesce(options)
            }
        }
    }
}