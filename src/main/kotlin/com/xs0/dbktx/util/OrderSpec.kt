package com.xs0.dbktx.util

import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.DbEntity

class OrderSpec<E : DbEntity<E, *>>(val expr: Expr<in E, *>, val isAscending: Boolean)
