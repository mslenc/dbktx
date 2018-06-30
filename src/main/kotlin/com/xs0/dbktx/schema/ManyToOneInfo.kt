package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprFields
import com.xs0.dbktx.expr.ExprOneOf
import com.xs0.dbktx.util.RemappingList
import kotlin.collections.ArrayList

class ManyToOneInfo<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TO_KEY : Any>(
    val manyTable: DbTable<FROM, *>,

    val oneTable: DbTable<TO, *>,
    val oneKey: UniqueKeyDef<TO, TO_KEY>,

    val columnMappings: Array<ColumnMapping<FROM, TO, *>>) {

    private val mappingToOneId = HashMap<Int, (FROM)->Any?>()

    init {
        val mappingToOneId = HashMap<Int, (FROM)->Any?>()
        for (mapping in columnMappings) {
            mappingToOneId[mapping.columnTo.indexInRow] = { entity ->
                doMapping(mapping, entity)
            }
        }
    }

    fun makeForwardMapper(): (FROM)->TO_KEY {
        return { fromEntity ->
            oneKey.invoke(RemappingList(mappingToOneId, fromEntity))
        }
    }

    internal fun <T : Any> doMapping(mapping: ColumnMapping<FROM, TO, T>, source: FROM): Any? {
        val value = mapping.columnFrom(source)
        val targetField = mapping.columnTo
        return targetField.sqlType.toJson(value!!)
    }

    fun makeReverseQueryBuilder(): (Set<TO_KEY>, TableInQuery<FROM>) -> ExprBoolean {
        if (columnMappings.size > 1) {
            // by construction, the column order in columnMappings is the same as the
            // one in TIDs

            return { idSet, tableInQuery ->
                @Suppress("UNCHECKED_CAST")
                val fields = ExprFields<FROM, TO_KEY>(columnMappings as Array<ColumnMapping<*, *, *>>, tableInQuery)

                if (idSet.isEmpty())
                    throw IllegalArgumentException()

                @Suppress("UNCHECKED_CAST") // composite ids are Expr themselves..
                idSet as Set<Expr<FROM, TO_KEY>>

                ExprOneOf(fields, ArrayList(idSet))
            }
        } else {
            @Suppress("UNCHECKED_CAST")
            val columnFrom = columnMappings[0].columnFrom as Column<FROM, TO_KEY>

            return { idsSet, tableInQuery ->
                if (idsSet.isEmpty())
                    throw IllegalArgumentException()

                ExprOneOf(columnFrom.bindForSelect(tableInQuery), idsSet.map { columnFrom.makeLiteral(it) })
//                column oneOf idSet
            }
        }
    }
}
