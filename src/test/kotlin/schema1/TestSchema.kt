package schema1

import com.xs0.dbktx.schema.DbSchema

object TestSchema: DbSchema() {
    val PEOPLE = DbPeople.TABLE
    val TAGS = DbTags.TABLE

    init { this.finishInit() }
}