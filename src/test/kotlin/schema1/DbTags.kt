package schema1

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.fieldprops.*

class DbTags(override val id: Int,
             private val row: List<Any?>) : DbEntity<DbTags, Int>() {

    override val metainfo = TABLE

    val tag: String get() = TAG(row)
    val ownerId: Int get() = OWNER_ID(row)

    companion object TABLE : DbTable<DbTags, Int>(TestSchema, "tags", DbTags::class, Int::class) {
        val ID = b.nonNullInt("id", INT(), DbTags::id, primaryKey = true, autoIncrement = true)
        val TAG = b.nonNullString("tag", VARCHAR(255), DbTags::tag)
        val OWNER_ID = b.nonNullInt("owner_id", INT(), DbTags::ownerId, references = DbPeople::class)

        val OWNER_REF = b.relToOne<DbPeople, Int>(OWNER_ID)

        init {
            b.build(::DbTags)
        }

        fun filter(builder: TABLE.() -> ExprBoolean<DbTags>): ExprBoolean<DbTags> {
            return this.builder()
        }
    }
}
