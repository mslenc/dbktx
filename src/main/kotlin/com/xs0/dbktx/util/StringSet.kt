package com.xs0.dbktx.util

class StringSet : LinkedHashSet<String> {
    constructor(initialCapacity: Int, loadFactor: Float) : super(initialCapacity, loadFactor)
    constructor(initialCapacity: Int) : super(initialCapacity)
    constructor() : super()
    constructor(c: Collection<String>?) : super(c)
}

fun Set<String>.toStringSet(): StringSet {
    return StringSet(this)
}
