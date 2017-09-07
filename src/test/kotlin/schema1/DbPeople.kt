package schema1

import com.xs0.dbktx.DbEntity
import com.xs0.dbktx.DbTable
import com.xs0.dbktx.fieldprops.*

class DbPeople(override val id: Int,
               private val row: List<Any?>) : DbEntity<DbPeople, Int>() {

    override val metainfo = TABLE

    val name: String get() = NAME.from(row)

    companion object TABLE : DbTable<DbPeople, Int>(TestSchema, "people", DbPeople::class, Int::class) {
        val ID = b.nonNullInt("id", INT(), { x -> x.id }, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), { x -> x.name })

        val TAGS_SET = b.relToMany { DbTags.OWNER_REF }

        init {
            b.build({ id, row -> DbPeople(id, row) })
        }
    }
}
