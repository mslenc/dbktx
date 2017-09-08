package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.schema.NonNullColumn
import com.xs0.dbktx.util.Sql

class MultiColOneOf<FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any>
    : ExprBoolean<FROM> {

    private val info: ManyToOneInfo<FROM, FROMID, TO, TOID>
    private val refs: List<TO>

    constructor(info: ManyToOneInfo<FROM, FROMID, TO, TOID>, refs: List<TO>) {
        this.info = info
        this.refs = refs
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            paren {
                tuple(info.columnMappings) {
                    +it.columnFrom
                }
            }

            +" IN "

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


