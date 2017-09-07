package schema1

import com.xs0.dbktx.DbEntity
import com.xs0.dbktx.DbTable
import com.xs0.dbktx.fieldprops.*

class DbPeople(override val id: Int,
               private val row: List<Any?>) : DbEntity<DbPeople, Int>() {

    override val metainfo = TABLE

    val firstName: String get() = FIRST_NAME(row)
    val lastName: String get() = LAST_NAME(row)
    val email: String get() = EMAIL(row)

    companion object TABLE : DbTable<DbPeople, Int>(TestSchema, "people", DbPeople::class, Int::class) {
        val ID = b.nonNullInt("id", INT(), { x -> x.id }, primaryKey = true, autoIncrement = true)
        val FIRST_NAME = b.nonNullString("firstName", VARCHAR(255), DbPeople::firstName)
        val LAST_NAME = b.nonNullString("lastName", VARCHAR(255), DbPeople::lastName)
        val EMAIL = b.nonNullString("email", VARCHAR(255), DbPeople::email)

        val TAGS_SET = b.relToMany { DbTags.OWNER_REF }

        init {
            b.build(::DbPeople)
        }
    }
}
