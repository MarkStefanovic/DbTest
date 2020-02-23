import kotlinx.serialization.Serializable

@Serializable
sealed class TestResult {
    abstract val testDescription: String
    abstract val executionTimeMillis: Long
    abstract val flex: Float
    abstract val flexPercent: Float
    abstract val mostly: Float

    @Serializable
    sealed class Passed : TestResult() {
        abstract override val testDescription: String

        @Serializable
        data class ValuesOK(
            override val testDescription: String,
            override val executionTimeMillis: Long,
            override val flex: Float,
            override val flexPercent: Float,
            override val mostly: Float,
            val datasourceName: String,
            val tableName: String,
            val fieldName: String,
            val sql: String
        ): Passed()

        @Serializable
        data class ValueComparisonOK(
            override val testDescription: String,
            override val executionTimeMillis: Long,
            override val flex: Float,
            override val flexPercent: Float,
            override val mostly: Float,
            val sourceDatasourceName: String,
            val sourceTableName: String,
            val sourceFieldName: String,
            val sourceSql: String,
            val destinationDatasourceName: String,
            val destinationTableName: String,
            val destinationFieldName: String,
            val destinationSql: String
        ): Passed()

        @Serializable
        data class RowsOK(
            override val testDescription: String,
            override val executionTimeMillis: Long,
            override val flex: Float,
            override val flexPercent: Float,
            override val mostly: Float,
            val datasourceName: String,
            val tableName: String,
            val sql: String
        ): Passed()

        @Serializable
        data class RowComparisonOK(
            override val testDescription: String,
            override val executionTimeMillis: Long,
            override val flex: Float,
            override val flexPercent: Float,
            override val mostly: Float,
            val sourceDatasourceName: String,
            val sourceTableName: String,
            val sourceSql: String,
            val destinationDatasourceName: String,
            val destinationTableName: String,
            val destinationSql: String
        ): Passed()
    }

    @Serializable
    sealed class Failed: TestResult() {
        abstract override val testDescription: String
        abstract val errorMessage: String

        @Serializable
        sealed class RowCount: Failed() {
            abstract override val testDescription: String
            abstract override val executionTimeMillis: Long
            abstract override val flex: Float
            abstract override val flexPercent: Float
            abstract override val mostly: Float
            abstract override val errorMessage: String
            abstract val datasourceName: String
            abstract val tableName: String
            abstract val sql: String

            @Serializable
            data class DoesNotEqual(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val datasourceName: String,
                override val tableName: String,
                override val sql: String,
                val expectedRowCount: Int,
                val actualRowCount: Int
            ) : RowCount() {
                override val errorMessage = "Expected $expectedRowCount rows, but got $actualRowCount"
            }

            @Serializable
            data class OutOfBounds(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val datasourceName: String,
                override val tableName: String,
                override val sql: String,
                val minExpectedRows: Int,
                val maxExpectedRows: Int,
                val actualRowCount: Int
            ) : RowCount() {
                override val errorMessage =
                    "Expected rows to be between $minExpectedRows and $maxExpectedRows rows, but got $actualRowCount rows"
            }

            @Serializable
            data class TooManyRows(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val datasourceName: String,
                override val tableName: String,
                override val sql: String,
                val maxExpectedRows: Int,
                val actualRowCount: Int
            ) : RowCount() {
                override val errorMessage = "Expected rows to be less than $maxExpectedRows rows, but got $actualRowCount rows"
            }

            @Serializable
            data class TooFewRows(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val datasourceName: String,
                override val tableName: String,
                override val sql: String,
                val minExpectedRows: Int,
                val actualRowCount: Int
            ) : RowCount() {
                override val errorMessage = "Expected more than $minExpectedRows rows, but got $actualRowCount rows"
            }
        }

        @Serializable
        sealed class RowCounts: Failed() {
            abstract override val testDescription: String
            abstract override val executionTimeMillis: Long
            abstract override val flex: Float
            abstract override val flexPercent: Float
            abstract override val mostly: Float
            abstract override val errorMessage: String
            abstract val sourceDatasourceName: String
            abstract val sourceTableName: String
            abstract val sourceSql: String
            abstract val destinationDatasourceName: String
            abstract val destinationTableName: String
            abstract val destinationSql: String

            @Serializable
            data class DoNotMatch(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val sourceDatasourceName: String,
                override val sourceTableName: String,
                override val sourceSql: String,
                override val destinationDatasourceName: String,
                override val destinationTableName: String,
                override val destinationSql: String,
                val sourceRows: Int,
                val destinationRows: Int
            ) : RowCounts() {
                override val errorMessage: String = "$sourceTableName rows do not match $destinationTableName rows."
            }
        }

        @Serializable
        sealed class InvalidValues : Failed() {
            abstract override val testDescription: String
            abstract override val executionTimeMillis: Long
            abstract override val flex: Float
            abstract override val flexPercent: Float
            abstract override val mostly: Float
            abstract override val errorMessage: String
            abstract val datasourceName: String
            abstract val tableName: String
            abstract val fieldName: String
            abstract val falsifyingExamples: Set<*>
            abstract val sql: String

            @Serializable
            data class MissingPrefix(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val prefix: String,
                val caseSensitive: Boolean
            ) : InvalidValues() {
                override val errorMessage: String = "One or more values were missing the prefix '$prefix'."
            }

            @Serializable
            data class MissingSuffix(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val suffix: String,
                val caseSensitive: Boolean
            ) : InvalidValues() {
                override val errorMessage = "One or more values were missing the suffix '$suffix'."
            }

            @Serializable
            data class NotLike(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val fragment: String,
                val caseSensitive: Boolean
            ) : InvalidValues() {
                override val errorMessage = "One or more values did not contain the fragment, '$fragment'."
            }

            @Serializable
            data class NotOneOf<V>(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<V>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val expectedValues: Set<V>,
                val caseSensitive: Boolean
            ) : InvalidValues() {
                override val errorMessage = "The test returned unexpected values."
            }

            @Serializable
            data class OutOfBounds<V>(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<V>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val minExpectedValue: V,
                val maxExpectedValue: V
            ) : InvalidValues() {
                override val errorMessage = "The test returned unexpected values."
            }

            @Serializable
            data class TooLarge<V>(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<V>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val maxExpectedValue: V
            ) : InvalidValues() {
                override val errorMessage = "One or more values were larger than $maxExpectedValue."
            }

            @Serializable
            data class TooSmall<V>(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<V>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val minExpectedValue: V
            ) : InvalidValues() {
                override val errorMessage = "One or more values were smaller than $minExpectedValue."
            }

            @Serializable
            data class TooShort(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val minLength: Int
            ) : InvalidValues() {
                override val errorMessage = "One or more values had lengths less than $minLength characters."
            }

            @Serializable
            data class TooLong(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val maxLength: Int
            ) : InvalidValues() {
                override val errorMessage = "One or more values had lengths more than $maxLength characters."
            }

            @Serializable
            data class TooShortOrTooLong(
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val falsifyingExamples: Set<String>,
                override val datasourceName: String,
                override val tableName: String,
                override val fieldName: String,
                override val sql: String,
                val minLength: Int,
                val maxLength: Int
            ) : InvalidValues() {
                override val errorMessage =
                    "One or more values had lengths less than $minLength or more than $maxLength characters."
            }
        }

        @Serializable
        sealed class ValuesDontMatch : Failed() {
            abstract override val testDescription: String
            abstract override val executionTimeMillis: Long
            abstract override val flex: Float
            abstract override val flexPercent: Float
            abstract override val mostly: Float
            abstract override val errorMessage: String
            abstract val sourceDatasourceName: String
            abstract val sourceTableName: String
            abstract val sourceFieldName: String
            abstract val sourceSql: String
            abstract val destinationDatasourceName: String
            abstract val destinationTableName: String
            abstract val destinationFieldName: String
            abstract val destinationSql: String

            @Serializable
            data class TotalsDontMatch <N: Number> (
                override val testDescription: String,
                override val executionTimeMillis: Long,
                override val flex: Float,
                override val flexPercent: Float,
                override val mostly: Float,
                override val sourceDatasourceName: String,
                override val destinationDatasourceName: String,
                override val sourceTableName: String,
                override val destinationTableName: String,
                override val sourceFieldName: String,
                override val destinationFieldName: String,
                override val sourceSql: String,
                override val destinationSql: String,
                val sourceTotal: N,
                val destinationTotal: N
            ) : ValuesDontMatch() {
                override val errorMessage = "$sourceTableName.$sourceFieldName = $sourceTotal, " +
                    "but $destinationTableName.$destinationFieldName = $destinationTotal."
            }
        }
    }
}
