package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprFields
import com.xs0.dbktx.expr.ExprOneOf
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.LinkedHashSet

class ManyToOneInfo<FROM : DbEntity<FROM, FID>, FID : Any, TO : DbEntity<TO, TID>, TID : Any>(
        val manyTable: DbTable<FROM, FID>,
        val oneTable: DbTable<TO, TID>,
        val columnMappings: Array<ColumnMapping<FROM, TO, *>>) {

    fun makeForwardMapper(): (FROM)->TID {
        return { source ->
            val row = arrayOfNulls<Any>(oneTable.numColumns)

            for (mapping in columnMappings)
                doMapping(mapping, source, row)

            oneTable.createId(row.toList())
        }
    }

    internal fun <T : Any> doMapping(mapping: ColumnMapping<FROM, TO, T>, source: FROM, row: Array<Any?>) {
        val value = mapping.columnFrom(source)

        val targetField = mapping.columnTo
        row[targetField.indexInRow] = targetField.sqlType.toJson(value!!)
    }

    fun makeReverseQueryBuilder(): (Set<TID>, TableInQuery<FROM>) -> ExprBoolean {
        if (columnMappings.size > 1) {
            // by construction, the column order in columnMappings is the same as the
            // one in TIDs

            return { idSet, tableInQuery ->
                val sb = StringBuilder()
                var i = 0
                val n = columnMappings.size
                while (i < n) {
                    sb.append(if (i == 0) "(" else ", ")
                    sb.append(tableInQuery.tableAlias).append('.')
                    sb.append(columnMappings[i].columnFrom.quotedFieldName)
                    i++
                }
                sb.append(")")

                val fields = ExprFields<FROM, TID>(sb.toString())

                if (idSet.isEmpty())
                    throw IllegalArgumentException()

                @Suppress("UNCHECKED_CAST") // composite ids are Expr..
                idSet as Set<Expr<FROM, TID>>

                ExprOneOf(fields, ArrayList(idSet))
            }
        } else {
            val column = columnMappings[0].columnFrom

            @Suppress("UNCHECKED_CAST")
            column as RowProp<FROM, TID>

            return { idSet, tableInQuery ->
                if (idSet.isEmpty())
                    throw IllegalArgumentException()

                ExprOneOf(column.bindForSelect(tableInQuery), idSet.map { column.makeLiteral(it) })
//                column oneOf idSet
            }
        }
    }
}
