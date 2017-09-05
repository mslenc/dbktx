package com.xs0.dbktx

fun escapePattern(pattern: String, escapeChar: Char): String {
    // lazy allocate, because most patterns are not expected to actually contain
    // _ or %
    var sb: StringBuilder? = null

    var i = 0
    val n = pattern.length
    while (i < n) {
        val c = pattern[i]
        if (c == '%' || c == '_' || c == escapeChar) {
            if (sb == null) {
                sb = StringBuilder(pattern.length + 16)
                sb.append(pattern, 0, i)
            }
            sb.append(escapeChar)
        }
        if (sb != null)
            sb.append(c)
        i++
    }

    return if (sb != null) sb.toString() else pattern
}

interface ExprString<TABLE> : Expr<TABLE, String> {
    fun contains(value: String): ExprBoolean<TABLE> {
        return like("%" + escapePattern(value, '|') + "%", '|')
    }

    fun startsWith(value: String): ExprBoolean<TABLE> {
        return like(escapePattern(value, '|') + "%", '|')
    }

    fun endsWith(value: String): ExprBoolean<TABLE> {
        return like("%" + escapePattern(value, '|'), '|')
    }

    fun like(pattern: String, escapeChar: Char): ExprBoolean<TABLE> {
        return ExprLike(this, SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun like(pattern: Expr<in TABLE, String>, escapeChar: Char): ExprBoolean<TABLE> {
        return ExprLike(this, pattern, escapeChar)
    }
}