package com.xs0.dbktx

class ExprRel<TABLE, T>(private val left: Expr<in TABLE, T>, private val relation: Rel, private val right: Expr<in TABLE, T>) : ExprBoolean<TABLE> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        left.toSql(sb, false)
        sb.sql(" ").sql(relation.sqlOperator).sql(" ")
        right.toSql(sb, false)

        sb.closeParen(topLevel)
    }
}

enum class Rel constructor(val sqlOperator: String) {
    LESS("<"),
    LESS_OR_EQUAL("<="),
    GREATER(">"),
    GREATER_OR_EQUAL(">=")
}
