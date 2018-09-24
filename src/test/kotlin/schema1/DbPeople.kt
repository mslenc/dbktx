package schema1

import com.github.mslenc.asyncdb.common.RowData
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.fieldprops.*

class DbPeople(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<DbPeople, Int>(db, id) {

    override val metainfo = TABLE

    val firstName: String get() = FIRST_NAME(row)
    val lastName: String get() = LAST_NAME(row)
    val email: String get() = EMAIL(row)

    suspend fun tags(): List<DbTags> = TAGS_SET(this)

    suspend fun tags(filterBuilder: FilterBuilder<DbTags>.() -> ExprBoolean): List<DbTags> {
        return db.load(this, TAGS_SET, filterBuilder)
    }

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
