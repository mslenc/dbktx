package schema1

import com.xs0.asyncdb.common.RowData
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.fieldprops.*

class DbTags(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<DbTags, Int>(db, id) {

    override val metainfo = TABLE

    val tag: String get() = TAG(row)
    val ownerId: Int get() = OWNER_ID(row)

    companion object TABLE : DbTable<DbTags, Int>(TestSchema, "tags", DbTags::class, Int::class) {
        val ID = b.nonNullInt("id", INT(), DbTags::id, primaryKey = true, autoIncrement = true)
        val TAG = b.nonNullString("tag", VARCHAR(255), DbTags::tag)
        val OWNER_ID = b.nonNullInt("owner_id", INT(), DbTags::ownerId)

        val OWNER_REF = b.relToOne(OWNER_ID, DbPeople::class)

        init {
            b.build(::DbTags)
        }
    }
}
