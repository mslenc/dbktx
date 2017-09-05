package com.xs0.dbktx

interface Sqlable<T> {
    fun toSql(value: T, sb: SqlBuilder, topLevel: Boolean)
}