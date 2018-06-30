package com.xs0.dbktx.util

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import java.util.*

class KeyTuple<E: DbEntity<E, *>>(val table: DbTable<E, *>, vararg val parts: Any) {
    override fun equals(other: Any?): Boolean {
        if (other !is KeyTuple<*>)
            return false

        if (parts.size != other.parts.size)
            return false

        for (i in 0 until parts.size)
            if (parts[i] != other.parts[i])
                return false

        return true
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(parts)
    }

    override fun toString(): String {
        return Arrays.toString(parts)
    }
}