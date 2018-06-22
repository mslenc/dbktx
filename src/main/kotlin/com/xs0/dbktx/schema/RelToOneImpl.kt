package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.*

class RelToOneImpl<FROM : DbEntity<FROM, FROMID>, FROMID : Any, TO : DbEntity<TO, TOID>, TOID : Any> : RelToOne<FROM, TO> {
    internal lateinit var info: ManyToOneInfo<FROM, FROMID, TO, TOID>
    internal lateinit var idMapper: (FROM)->TOID?

    fun init(info: ManyToOneInfo<FROM, FROMID, TO, TOID>, idMapper: (FROM)->TOID?) {
        this.info = info
        this.idMapper = idMapper
    }

    fun mapId(from: FROM): TOID? {
        return idMapper(from)
    }

    override val targetTable: DbTable<TO, TOID>
        get() = info.oneTable

    override suspend fun invoke(from: FROM): TO? {
        return from.db.find(from, this)
    }

    companion object {
        internal fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, T: Any>
            makeEq(map: ColumnMapping<FROM, TO, T>, ref: TO, tableInQuery: TableInQuery<FROM>): ExprBoolean {

            return ExprBinary(
                map.columnFrom.bindForSelect(tableInQuery),
                ExprBinary.Op.EQ,
                map.columnFrom.makeLiteral(map.columnTo(ref))
            )
        }

        internal fun <FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, T: Any>
            makeOneOf(map: ColumnMapping<FROM, TO, T>, refs: List<TO>, tableInQuery: TableInQuery<FROM>): ExprBoolean {

            val set = LinkedHashSet<T>(refs.size)
            refs.mapTo(set) { map.columnTo(it) }

            return ExprOneOf(map.columnFrom.bindForSelect(tableInQuery), set.map { map.columnFrom.makeLiteral(it) })
        }
    }
}
