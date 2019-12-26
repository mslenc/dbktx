package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueNull
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprFields
import com.github.mslenc.dbktx.filters.FilterOneOf
import com.github.mslenc.dbktx.util.RemappingList

class ManyToOneInfo<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TO_KEY : Any>(
    val manyTable: DbTable<FROM, *>,

    val oneTable: DbTable<TO, *>,
    val oneKey: UniqueKeyDef<TO, TO_KEY>,

    val columnMappings: Array<ColumnMapping<FROM, TO, *>>) {

    private val mappingToOneId = HashMap<Int, (FROM)->DbValue?>()

    init {
        for (mapping in columnMappings) {
            mappingToOneId[mapping.rawColumnTo.indexInRow] = { entity ->
                doMapping(mapping, entity)
            }
        }
    }

    fun makeForwardMapper(): (FROM)->TO_KEY {
        return { fromEntity ->
            oneKey.invoke(RemappingList(mappingToOneId, fromEntity))
        }
    }

    private fun <T : Any> doMapping(mapping: ColumnMapping<FROM, TO, T>, source: FROM): DbValue? {
        return if (mapping.columnFromKind == ColumnInMappingKind.COLUMN) {
            val value = mapping.rawColumnFrom(source)
            if (value == null) {
                DbValueNull.instance()
            } else {
                mapping.rawColumnTo.makeDbValue(value)
            }
        } else {
            mapping.rawColumnTo.makeDbValue(mapping.rawLiteralFromValue)
        }
    }

    fun makeReverseQueryBuilder(): (Set<TO_KEY>, TableInQuery<FROM>)->Expr<Boolean> {
        if (columnMappings.size > 1) {
            // by construction, the column order in columnMappings is the same as the
            // one in TIDs

            return { idSet, tableInQuery ->
                @Suppress("UNCHECKED_CAST")
                val fields = ExprFields<FROM, TO_KEY>(columnMappings as Array<ColumnMapping<*, *, *>>, tableInQuery)

                if (idSet.isEmpty())
                    throw IllegalArgumentException()

                @Suppress("UNCHECKED_CAST") // composite ids are Expr themselves..
                idSet as Set<Expr<TO_KEY>>

                FilterOneOf(fields, idSet.toList())
            }
        } else {
            @Suppress("UNCHECKED_CAST")
            val columnFrom = columnMappings[0].rawColumnFrom as Column<FROM, TO_KEY> // (we don't allow all-constant refs, and if there's only one, it has to be a column)

            return { idsSet, tableInQuery ->
                if (idsSet.isEmpty())
                    throw IllegalArgumentException()

                FilterOneOf(columnFrom.bindForSelect(tableInQuery), idsSet.map { columnFrom.makeLiteral(it) })
//                column oneOf idSet
            }
        }
    }
}
