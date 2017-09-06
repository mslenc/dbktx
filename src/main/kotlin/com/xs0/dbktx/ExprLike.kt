package com.xs0.dbktx

class ExprLike<E> (
        value: Expr<in E, String>,
        pattern: Expr<in E, String>,
        escapeChar: Char = '|') : ExprBoolean<E> {

    private val value: Expr<in E, String> = value
    private val pattern: Expr<in E, String> = pattern
    private val escapeChar: Char = escapeChar

    init {
        if (escapeChar == '\'')
            throw IllegalArgumentException("Invalid escape char - it can't be '")
    }

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        value.toSql(sb, false)
        sb.sql(" LIKE ")
        pattern.toSql(sb, false)
        sb.sql(" ESCAPE '")
        sb.sql(escapeChar.toString())
        sb.sql("'")

        sb.closeParen(topLevel)
    }
}