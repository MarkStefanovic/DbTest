sealed class DbResult {
    abstract val executionTimeMillis: Long

    data class Single<V>(
        val values: List<V>,
        override val executionTimeMillis: Long
    ) : DbResult()

    data class Multiple<V>(
        val sourceValues: List<V>,
        val destinationValues: List<V>,
        override val executionTimeMillis: Long
    ) : DbResult()
}

