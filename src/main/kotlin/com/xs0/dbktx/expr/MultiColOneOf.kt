package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.schema.NonNullColumn
import com.xs0.dbktx.util.Sql

class MultiColOneOf<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    : ExprBoolean {

    private val info: ManyToOneInfo<FROM, *, TO, *>
    private val refs: List<TO>
    private val negated: Boolean
    private val tableInQuery: TableInQuery<FROM>

    constructor(tableInQuery: TableInQuery<FROM>, info: ManyToOneInfo<FROM, *, TO, *>, refs: List<TO>, negated: Boolean = false) {
        this.tableInQuery = tableInQuery
        this.info = info
        this.refs = refs
        this.negated = negated
    }

    override fun not(): ExprBoolean {
        return MultiColOneOf(tableInQuery, info, refs, !negated)
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            paren {
                tuple(info.columnMappings) {
                    columnForSelect(tableInQuery, it.columnFrom)
                }
            }

            +(if (negated) " NOT IN " else " IN ")

            paren {
                tuple(refs) { ref ->
                    paren {
                        tuple(info.columnMappings) { colMap ->
                            writeLiteral(colMap.columnTo, ref, sql)
                        }
                    }
                }
            }
        }
    }

    private fun <T: Any>
    writeLiteral(column: NonNullColumn<TO, T>, ref: TO, sql: Sql) {
        column.sqlType.toSql(column(ref), sql)
    }
}


