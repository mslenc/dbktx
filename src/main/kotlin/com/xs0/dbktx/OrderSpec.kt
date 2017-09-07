package com.xs0.dbktx

class OrderSpec<E : DbEntity<E, *>>(val expr: Expr<in E, *>, val isAscending: Boolean)
