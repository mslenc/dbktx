package schema1

import com.github.mslenc.dbktx.schema.DbSchema

object TestSchema: DbSchema() {
    val PEOPLE = DbPeople.TABLE
    val TAGS = DbTags.TABLE

    init { finishInit() }
}