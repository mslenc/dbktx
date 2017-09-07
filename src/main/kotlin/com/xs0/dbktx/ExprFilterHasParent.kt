package com.xs0.dbktx

class ExprFilterHasParent<FROM : DbEntity<FROM, FID>, FID : Any, TO : DbEntity<TO, TID>, TID : Any>(
        private val info: ManyToOneInfo<FROM, FID, TO, TID>,
        private val filter: Expr<TO, Boolean>) : ExprBoolean<FROM> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        val mappings = info.columnMappings
        val n = mappings.size

        for (i in 0 until n) {
            if (i == 0 && n > 1)
                sb.sql("(")
            if (i > 0)
                sb.sql(", ")
            sb.name(mappings[i].columnFrom)
            if (i == n - 1 && n > 1)
                sb.sql(")")
        }

        sb.sql(" IN (")

        sb.sql("SELECT ")
        for (i in 0 until n) {
            if (i > 0)
                sb.sql(", ")
            sb.name(mappings[i].columnTo)
        }

        sb.sql(" FROM ")
        sb.sql(info.oneTable.dbName)
        sb.sql(" WHERE ")
        filter.toSql(sb, true)

        sb.sql(")")

        sb.closeParen(topLevel)
    }
}
