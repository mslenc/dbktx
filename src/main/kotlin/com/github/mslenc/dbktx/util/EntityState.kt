package com.github.mslenc.dbktx.util

enum class EntityState {
    INITIAL,   // each entity starts here
    LOADING,   // the loader which will load this is in queue
    LOADED     // the entity is loaded
}