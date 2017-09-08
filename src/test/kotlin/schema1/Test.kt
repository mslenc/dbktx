package schema1

import com.xs0.dbktx.DbConn

import schema1.TestSchema.PEOPLE
import schema1.TestSchema.TAGS

suspend fun apiTest(db: DbConn) {


    val person: DbPeople = db.load(PEOPLE, 15)
    val tags: List<DbTags> = db.load(person, DbPeople.TAGS_SET)
    val dirty = db.query(TAGS, (DbTags.TAG contains "f*ck") and
                               (DbTags.OWNER_ID `==` person.id))

    val dirty2 = db.query(TAGS) { (TAG contains "f*ck") and
                                  (OWNER_ID eq person.id) }

    val dirtyOwners = db.query(PEOPLE) {
        (ID gte 10) and
        TAGS_SET.contains(TAGS.filter {
            TAG.contains("dirty") and
            (TAG neq TAG)

        })
    }
}