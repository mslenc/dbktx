package com.github.mslenc.dbktx.expr

enum class BinaryOp(val sql: String) {
    PLUS("+"),
    MINUS("-"),
    TIMES("*"),
    DIV("/"),
    REM("%")
}