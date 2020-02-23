import kotlinx.serialization.ContextualSerialization
import kotlinx.serialization.Serializable
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import javax.sql.DataSource
import kotlin.Exception
import kotlin.system.measureTimeMillis


// TODO 20200306 implement flex and flexPercent logic in all tests

private fun tableOrSubquery(tableName: String, subquery: String?): String =
    if (subquery == null) {
        tableName
    } else {
        "($subquery) AS t"
    }

private fun wrapValue(str: String, dataType: DataType, dialect: SQLDialect): String =
    when (dataType) {
        DataType.DATE -> "'$str'"
        DataType.DATETIME -> "'$str'"
        DataType.DECIMAL -> str
        DataType.FLOAT -> str
        DataType.INTEGER -> str
        DataType.TEXT -> str
    }

private fun wrapField(
    fieldName: String,
    dataType: DataType,
    dialect: SQLDialect
): String =
    if (dialect == SQLDialect.MSSQL) {
        "[$fieldName]"
    } else {
        "\"$fieldName\""
    }


fun validateTable(datasourceName: String, tableName: String): RuleValidationResult =
    IsValid
        .and("Datasource names cannot be blank.") { datasourceName.isBlank() }
        .and("Table names cannot be blank.") { tableName.isBlank() }

fun validateColumn(
    datasourceName: String,
    tableName: String,
    fieldName: String,
    maxFalsifyingExamples: Int
): RuleValidationResult =
    validateTable(
        datasourceName = datasourceName,
        tableName = tableName
    )
    .and("Field names cannot be blank.") {
        fieldName.isBlank()
    }
    .and("The maxFalsifyingExamples argument cannot be negative") {
        maxFalsifyingExamples < 0
    }


private fun LocalDateTime.toDateString(dataType: DataType): String =
    when (dataType) {
        DataType.DATE, DataType.DATETIME -> {
            this.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
//            try {
//                this.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
//            } catch (e: Exception) {
//                // TODO catch specific exception
//                throw IllegalArgumentException("Could not format '$this' as a ${dataType.name}.")
//            }
        }
        else -> throw IllegalArgumentException("Cannot format $this, data type $dataType, as a date string.")
    }

private fun <V> DataSource.getFirstColumnValues(sql: String, dataType: DataType): List<V> =
    serialTransaction(ds = this) {
        execAndMap(sql = sql) { rs ->
            when (dataType) {
                DataType.DATE -> rs.getDate(1).toLocalDate()
                DataType.DATETIME -> rs.getTimestamp(1).toLocalDateTime()
                DataType.DECIMAL -> rs.getBigDecimal(1)
                DataType.FLOAT -> rs.getFloat(1)
                DataType.INTEGER -> rs.getInt(1)
                DataType.TEXT -> rs.getString(1)
            } ?: EmptyResult
        } as? List<V> ?: throw InvalidDataTypeSpecification(dataType)
    }

inline fun <R> executeAndMeasureTimeMillis(block: () -> R): Pair<R, Long> {
    val start = System.currentTimeMillis()
    val result = block()
    return result to (System.currentTimeMillis() - start)
}

@Serializable
sealed class Rule {
    abstract val description: String
    abstract val flex: Float
    abstract val flexPercent: Float
    abstract val mostly: Float

    abstract fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult

    abstract fun validate(): RuleValidationResult

    @Serializable
    sealed class SingleTable : Rule() {
        abstract val datasourceName: String
        abstract val subquery: String?
        abstract val dialect: SQLDialect
        abstract val tableName: String
        abstract val sql: String

        override fun validate(): RuleValidationResult = validateTable(
            datasourceName = datasourceName,
            tableName = tableName
        )

        @Serializable
        sealed class Column<V> : SingleTable() {
            abstract val fieldName: String
            abstract val dataType: DataType
            abstract val maxFalsifyingExamples: Int
            abstract val predicateToFalsify: String

            abstract fun check(result: DbResult.Single<V>): TestResult

            override fun validate(): RuleValidationResult = validateColumn(
                datasourceName = datasourceName,
                tableName = tableName,
                fieldName = fieldName,
                maxFalsifyingExamples = maxFalsifyingExamples
            )

            internal val wrappedFieldName: String by lazy {
                wrapField(fieldName = fieldName, dataType = dataType, dialect = dialect)
            }

            override val sql: String by lazy {
                val from = tableOrSubquery(tableName = tableName, subquery = subquery)
                if (dialect == SQLDialect.MSSQL) {
                    "SELECT TOP ($maxFalsifyingExamples) $wrappedFieldName FROM $from WHERE $predicateToFalsify"
                } else {
                    "SELECT $wrappedFieldName FROM $from WHERE $predicateToFalsify LIMIT ($maxFalsifyingExamples)"
                }
            }

//            override fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult {
//                val ds = datasourceImplementations[datasourceName]
//                    ?: throw DatasourceNotFound(datasourceName = datasourceName)
//                val (result: List<V>, executionTimeMillis: Long) = executeAndMeasureTimeMillis {
//                    serialTransaction(ds = ds) {
//                        execAndMap(sql = sql) { rs ->
//                            when (dataType) {
//                                DataType.DATE -> {
//                                    if (dialect == SQLDialect.SQLITE) {
//                                        val str: String = rs.getString(fieldName)
//                                        LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay() as LocalDateTime
//                                    } else {
//                                        rs.getDate(fieldName).toLocalDate()
//                                    }
//                                }
//                                DataType.DATETIME -> {
//                                    if (dialect == SQLDialect.SQLITE) {
//                                        val str: String = rs.getString(fieldName)
//                                        LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME) as LocalDateTime
//                                    } else {
//                                        rs.getTimestamp(fieldName).toLocalDateTime()
//                                    }
//                                }
//                                DataType.DECIMAL -> rs.getBigDecimal(fieldName)
//                                DataType.FLOAT -> rs.getFloat(fieldName)
//                                DataType.INTEGER -> rs.getInt(fieldName)
//                                DataType.TEXT -> rs.getString(fieldName)
//                            } ?: EmptyResult
//                        } as? List<V> ?: throw InvalidDataTypeSpecification(dataType)
//                    }
//                }
//                return check(result = DbResult.Single(values = result, executionTimeMillis = executionTimeMillis))
//            }

            override fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult {
                val ds = datasourceImplementations[datasourceName]
                    ?: throw DatasourceNotFound(datasourceName = datasourceName)
                val (result: List<V>, executionTimeMillis: Long) = executeAndMeasureTimeMillis {
                    serialTransaction(ds = ds) {
                        execAndMap(sql = sql) { rs ->
                            when (dataType) {
                                DataType.DATE -> {
                                    if (dialect == SQLDialect.SQLITE) {
                                        val str: String = rs.getString(fieldName)
                                        try {
                                            LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay() as LocalDateTime
                                        } catch (e: DateTimeParseException) {
                                            LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME) as LocalDateTime
                                        }
                                    } else {
                                        rs.getDate(fieldName).toLocalDate()
                                    }
                                }
                                DataType.DATETIME -> {
                                    if (dialect == SQLDialect.SQLITE) {
                                        val str: String = rs.getString(fieldName)
                                        try {
                                            LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME) as LocalDateTime
                                        } catch (e: DateTimeParseException) {
                                            LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay() as LocalDateTime
                                        }
                                    } else {
                                        rs.getTimestamp(fieldName).toLocalDateTime()
                                    }
                                }
                                DataType.DECIMAL -> rs.getBigDecimal(fieldName)
                                DataType.FLOAT -> rs.getFloat(fieldName)
                                DataType.INTEGER -> rs.getInt(fieldName)
                                DataType.TEXT -> rs.getString(fieldName)
                            } ?: EmptyResult
                        } as? List<V> ?: throw InvalidDataTypeSpecification(dataType)
                    }
                }
                return check(result = DbResult.Single(values = result, executionTimeMillis = executionTimeMillis))
            }

            internal fun passed(executionTimeMillis: Long): TestResult.Passed.ValuesOK =
                TestResult.Passed.ValuesOK(
                    testDescription = description,
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    sql = sql,
                    flex = flex,
                    flexPercent = flexPercent,
                    mostly = mostly,
                    executionTimeMillis = executionTimeMillis
                )

            @Serializable
            sealed class Dates <DateType> : Column<@Serializable(with = LocalDateTimeSerializer::class) LocalDate>() {
                @Serializable
                data class ShouldBeAfter <DateType>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val dataType: DataType,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    @Serializable(with = LocalDateTimeSerializer::class) val date: LocalDateTime
                ) : Dates<DateType>() {

                    private val dateStr: String by lazy {
                        date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    }

                    override val description = "$fieldName should be after $dateStr"

                    override val predicateToFalsify =
                        "$wrappedFieldName <= ${wrapValue(str = dateStr, dataType = dataType, dialect = dialect)}"

                    override fun check(result: DbResult.Single<LocalDate>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooSmall(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                minExpectedValue = date,
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis
                            )
                        }
                }

                @Serializable
                data class ShouldBeOnOrAfter<DateType>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val dataType: DataType,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    @Serializable(with = LocalDateTimeSerializer::class) val date: LocalDateTime
                ) : Dates<DateType>() {

                    private val dateStr: String by lazy {
                        date.toDateString(dataType = dataType)
                    }

                    override val description = "$fieldName should be on or after $dateStr"

                    override val predicateToFalsify: String =
                        "$wrappedFieldName < ${wrapValue(str = dateStr, dataType = dataType, dialect = dialect)}"

                    override fun check(result: DbResult.Single<LocalDate>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooSmall(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                minExpectedValue = date
                            )
                        }
                }

                @Serializable
                data class ShouldBeBefore<DateType>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val dataType: DataType,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    @Serializable(with = LocalDateTimeSerializer::class) val date: LocalDateTime
                ) : Dates<DateType>() {

                    private val dateStr: String by lazy {
                        date.toDateString(dataType = dataType)
                    }

                    override val description = "$fieldName should be before $dateStr"

                    override val predicateToFalsify: String =
                        "$wrappedFieldName >= ${wrapValue(str = dateStr, dataType = dataType, dialect = dialect)}"

                    override fun check(result: DbResult.Single<LocalDate>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooLarge(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                maxExpectedValue = date
                            )
                        }
                }

                @Serializable
                data class ShouldBeOnOrBefore<DateType>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val dataType: DataType,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    @Serializable(with = LocalDateTimeSerializer::class) val date: LocalDateTime
                ) : Dates<DateType>() {

                    private val dateStr: String by lazy {
                        date.toDateString(dataType = dataType)
                    }

                    override val description = "$fieldName should be on or before $dateStr"

                    override val predicateToFalsify =
                        "$wrappedFieldName > ${wrapValue(str = dateStr, dataType = dataType, dialect = dialect)}"

                    override fun check(result: DbResult.Single<LocalDate>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooLarge(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                maxExpectedValue = date
                            )
                        }
                }

                @Serializable
                data class ShouldBeBetween<DateType>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val dataType: DataType,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    @Serializable(with = LocalDateTimeSerializer::class) val minDate: LocalDateTime,
                    @Serializable(with = LocalDateTimeSerializer::class) val maxDate: LocalDateTime
                ) : Dates<DateType>() {

                    private val minDateStr: String by lazy {
                        minDate.toDateString(dataType = dataType)
                    }

                    private val maxDateStr: String by lazy {
                        maxDate.toDateString(dataType = dataType)
                    }

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The minDate cannot be greater than the maxDate.") { minDate > maxDate }

                    override val description = "$fieldName should be between $minDateStr and $maxDateStr."

                    override val predicateToFalsify =
                        "$wrappedFieldName NOT BETWEEN " +
                            "${wrapValue(str = minDateStr, dataType = dataType, dialect = dialect)} AND " +
                            "${wrapValue(str = maxDateStr, dataType = dataType, dialect = dialect)}"

                    override fun check(result: DbResult.Single<LocalDate>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.OutOfBounds(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                minExpectedValue = minDate,
                                maxExpectedValue = maxDate
                            )
                        }
                }
            }

            @Serializable
            sealed class Text : Column<String>() {

                override val dataType = DataType.TEXT

                @Serializable
                data class ShouldBeLike(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    val fragment: String,
                    val caseSensitive: Boolean
                ) : Text() {

                    override val description: String = "Text should contain '$fragment'."

                    override val predicateToFalsify = "$wrappedFieldName NOT LIKE '%$fragment%'"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The text fragment cannot be blank.") { fragment.isBlank() }

                    override fun check(result: DbResult.Single<String>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.NotLike(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                fragment = fragment,
                                caseSensitive = caseSensitive
                            )
                        }
                }

                @Serializable
                data class ShouldStartWith(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    val prefix: String,
                    val caseSensitive: Boolean
                ) : Text() {

                    override val description = "$fieldName should start with '$prefix'."

                    override val predicateToFalsify: String = "$wrappedFieldName NOT LIKE '$prefix%'"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The prefix cannot be blank.") { prefix.isBlank() }

                    override fun check(result: DbResult.Single<String>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.MissingPrefix(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                prefix = prefix,
                                caseSensitive = caseSensitive
                            )
                        }
                }

                @Serializable
                data class ShouldEndWith(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    val suffix: String,
                    val caseSensitive: Boolean
                ) : Text() {

                    override val description = "$fieldName should end with '$suffix'."

                    override val predicateToFalsify: String = "$wrappedFieldName NOT LIKE '%$suffix'"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The suffix cannot be blank.") { suffix.isBlank() }


                    override fun check(result: DbResult.Single<String>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.MissingSuffix(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                suffix = suffix,
                                caseSensitive = caseSensitive
                            )
                        }
                }

                @Serializable
                data class ShouldBeOneOf(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    val caseSensitive: Boolean,
                    val values: Set<String>
                ) : Text() {

                    override val description =
                        "$fieldName values should be one of ${values.joinToString(", ")}}."

                    override val predicateToFalsify: String by lazy {
                        val sqlList: String = values.joinToString { "\'$it\'" }
                        "$wrappedFieldName NOT IN ($sqlList)"
                    }

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The list of values cannot be empty.") { values.isEmpty() }

                    override fun check(result: DbResult.Single<String>): TestResult =
                        if (values.containsAll(result.values)) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.NotOneOf(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                expectedValues = values,
                                caseSensitive = caseSensitive
                            )
                        }
                }

                @Serializable
                data class LengthsShouldBeBetween(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    val minLength: Int,
                    val maxLength: Int
                ) : Text() {

                    override val description = "$fieldName length should be between $minLength and $maxLength."

                    override val predicateToFalsify: String =
                        "LENGTH($wrappedFieldName) NOT BETWEEN $minLength AND $maxLength"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("maxLength cannot be less than minLength.") { maxLength < minLength }

                    override fun check(result: DbResult.Single<String>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooShortOrTooLong(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                minLength = minLength,
                                maxLength = maxLength
                            )
                        }
                }
            }

            @Serializable
            sealed class Numbers<N> : Column<N>()
                where N : Number, N : Comparable<N> {

                @Serializable
                data class ShouldBeAtLeast<N>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    override val dataType: DataType,
                    val minValue: N
                ) : Numbers<N>() where N : Number, N : Comparable<N> {

                    override val description = "$fieldName should be at least $minValue."

                    override val predicateToFalsify: String = "$wrappedFieldName < $minValue"

                    override fun check(result: DbResult.Single<N>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooSmall(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                minExpectedValue = minValue
                            )
                        }
                }

                @Serializable
                data class ShouldBeAtMost<N>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    override val dataType: DataType,
                    val maxValue: N
                ) : Numbers<N>() where N : Number, N : Comparable<N> {

                    override val description = "$fieldName should be at most $maxValue."

                    override val predicateToFalsify: String = "$wrappedFieldName > $maxValue"

                    override fun check(result: DbResult.Single<N>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.TooLarge(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                maxExpectedValue = maxValue
                            )
                        }
                }

                @Serializable
                data class ShouldBeBetween<N>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    override val dataType: DataType,
                    val minValue: N,
                    val maxValue: N
                ) : Numbers<N>() where N : Number, N : Comparable<N> {

                    override val description = "$fieldName should be between $minValue and $maxValue."

                    override val predicateToFalsify: String = "$wrappedFieldName NOT BETWEEN $minValue AND $maxValue"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("The maxValue cannot be less than the minValue.") { maxValue < minValue }

                    override fun check(result: DbResult.Single<N>): TestResult =
                        if (result.values.isEmpty()) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.OutOfBounds(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                minExpectedValue = minValue,
                                maxExpectedValue = maxValue
                            )
                        }
                }

                @Serializable
                data class ShouldBeOneOf<N>(
                    override val datasourceName: String,
                    override val subquery: String?,
                    override val dialect: SQLDialect,
                    override val tableName: String,
                    override val fieldName: String,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float,
                    override val mostly: Float,
                    override val dataType: DataType,
                    val values: Set<N>
                ) : Numbers<N>() where N : Number, N : Comparable<N> {

                    override val description = "$fieldName should be one of ${values.joinToString(", ")}."

                    override val predicateToFalsify: String = "$wrappedFieldName NOT IN (${values.joinToString(", ")})"

                    override fun validate(): RuleValidationResult =
                        super.validate()
                        .and("No values were provided.") { values.isEmpty() }

                    override fun check(result: DbResult.Single<N>): TestResult =
                        if (values.containsAll(result.values)) {
                            super.passed(executionTimeMillis = result.executionTimeMillis)
                        } else {
                            TestResult.Failed.InvalidValues.NotOneOf(
                                testDescription = description,
                                datasourceName = datasourceName,
                                tableName = tableName,
                                fieldName = fieldName,
                                sql = sql,
                                falsifyingExamples = result.values.toSet().take(maxFalsifyingExamples).toSortedSet(),
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                expectedValues = values,
                                caseSensitive = false
                            )
                        }
                }
            }
        }

        @Serializable
        sealed class Rows : SingleTable() {
            abstract fun check(result: DbResult.Single<Int>): TestResult

            override val sql: String by lazy {
                "SELECT COUNT(*) AS row_ct " +
                    "FROM ${tableOrSubquery(tableName = tableName, subquery = subquery)}"
            }

            override fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult {
                val (result: List<Int>, executionTimeMillis: Long) = executeAndMeasureTimeMillis {
                    val ds: DataSource = datasourceImplementations[datasourceName]
                        ?: throw DatasourceNotFound(datasourceName = datasourceName)
                    serialTransaction(ds = ds) {
                        execAndMap(sql = sql) { rs ->
                            rs.getInt(1)
                        }
                    }
                }
                return check(result = DbResult.Single(values = result, executionTimeMillis = executionTimeMillis))
            }

            internal fun passed(executionTimeMillis: Long): TestResult.Passed.RowsOK =
                TestResult.Passed.RowsOK(
                    testDescription = description,
                    datasourceName = datasourceName,
                    tableName = tableName,
                    sql = sql,
                    flex = flex,
                    flexPercent = flexPercent,
                    mostly = mostly,
                    executionTimeMillis = executionTimeMillis
                )

            @Serializable
            data class ShouldBeBetween(
                override val datasourceName: String,
                override val tableName: String,
                override val subquery: String?,
                override val dialect: SQLDialect,
                override val flex: Float,
                override val flexPercent: Float,
                val minRows: Int,
                val maxRows: Int
            ) : Rows() {

                override val mostly: Float = 1f

                override val description =
                    "$tableName rows should be between $minRows and $maxRows."

                override fun validate(): RuleValidationResult =
                    super.validate()
                    .and("The maxRows must be greater than the minRows.") { maxRows < minRows }

                override fun check(result: DbResult.Single<Int>): TestResult {
                    val actualRowCount = result.values.first()
                    return if (actualRowCount in minRows..maxRows) {
                        super.passed(executionTimeMillis = result.executionTimeMillis)
                    } else {
                        TestResult.Failed.RowCount.OutOfBounds(
                            testDescription = description,
                            datasourceName = datasourceName,
                            tableName = tableName,
                            sql = sql,
                            flex = flex,
                            flexPercent = flexPercent,
                            mostly = mostly,
                            executionTimeMillis = result.executionTimeMillis,
                            minExpectedRows = minRows,
                            maxExpectedRows = maxRows,
                            actualRowCount = actualRowCount
                        )
                    }
                }
            }

            @Serializable
            data class ShouldBeAtLeast(
                override val datasourceName: String,
                override val tableName: String,
                override val subquery: String?,
                override val dialect: SQLDialect,
                override val flex: Float,
                override val flexPercent: Float,
                val minRows: Int
            ) : Rows() {

                override val mostly: Float = 1f

                override val description = "$tableName rows should be at least $minRows rows."

                override fun validate(): RuleValidationResult =
                    super.validate()
                    .and("The minimum rows argument must be >= 0.") { minRows < 0 }

                override fun check(result: DbResult.Single<Int>): TestResult {
                    val actualRowCount = result.values.first()
                    return if (actualRowCount >= minRows) {
                        super.passed(executionTimeMillis = result.executionTimeMillis)
                    } else {
                        TestResult.Failed.RowCount.TooFewRows(
                            testDescription = description,
                            datasourceName = datasourceName,
                            tableName = tableName,
                            sql = sql,
                            flex = flex,
                            flexPercent = flexPercent,
                            mostly = mostly,
                            executionTimeMillis = result.executionTimeMillis,
                            minExpectedRows = minRows,
                            actualRowCount = actualRowCount
                        )
                    }
                }
            }

            @Serializable
            data class ShouldBeAtMost(
                override val datasourceName: String,
                override val tableName: String,
                override val subquery: String?,
                override val dialect: SQLDialect,
                override val flex: Float,
                override val flexPercent: Float,
                val maxRows: Int
            ) : Rows() {

                override val mostly: Float = 1f

                override val description = "$tableName rows should be at most $maxRows rows."

                override fun validate(): RuleValidationResult =
                    super.validate()
                    .and("The maximum rows argument must be >= 0.") { maxRows < 0 }

                override fun check(result: DbResult.Single<Int>): TestResult {
                    val actualRowCount = result.values.first()
                    return if (actualRowCount <= maxRows) {
                        super.passed(executionTimeMillis = result.executionTimeMillis)
                    } else {
                        TestResult.Failed.RowCount.TooManyRows(
                            testDescription = description,
                            datasourceName = datasourceName,
                            tableName = tableName,
                            sql = sql,
                            flex = flex,
                            flexPercent = flexPercent,
                            mostly = mostly,
                            executionTimeMillis = result.executionTimeMillis,
                            maxExpectedRows = maxRows,
                            actualRowCount = actualRowCount
                        )
                    }
                }
            }

            @Serializable
            data class ShouldEqual(
                override val datasourceName: String,
                override val tableName: String,
                override val subquery: String?,
                override val dialect: SQLDialect,
                override val flex: Float,
                override val flexPercent: Float,
                val rows: Int
            ) : Rows() {

                override val mostly: Float = 1f

                override val description = "$tableName rows should be at least $rows."

                override fun validate(): RuleValidationResult =
                    super.validate()
                    .and("The rows argument must be >= 0.") { rows < 0 }

                override fun check(result: DbResult.Single<Int>): TestResult {
                    val actualRowCount = result.values.first()
                    return if (actualRowCount == rows) {
                        super.passed(executionTimeMillis = result.executionTimeMillis)
                    } else {
                        TestResult.Failed.RowCount.DoesNotEqual(
                            testDescription = description,
                            datasourceName = datasourceName,
                            tableName = tableName,
                            sql = sql,
                            flex = flex,
                            flexPercent = flexPercent,
                            mostly = mostly,
                            executionTimeMillis = result.executionTimeMillis,
                            expectedRowCount = rows,
                            actualRowCount = actualRowCount
                        )
                    }
                }
            }
        }
    }

    @Serializable
    sealed class MultiTable : Rule() {
        abstract val sourceSQL: String
        abstract val destinationSQL: String

        @Serializable
        sealed class Rows : MultiTable() {
            abstract val sourceTable: Table
            abstract val destinationTable: Table

            override val sourceSQL: String by lazy {
                "SELECT COUNT(*) AS row_ct " +
                    "FROM ${tableOrSubquery(tableName = sourceTable.tableName, subquery = sourceTable.subquery)}"
            }

            override val destinationSQL: String by lazy {
                "SELECT COUNT(*) AS row_ct " +
                    "FROM ${tableOrSubquery(
                        tableName = destinationTable.tableName,
                        subquery = destinationTable.subquery
                    )}"
            }

            override fun validate(): RuleValidationResult {
                val sourceTableIsValid = validateTable(
                    datasourceName = sourceTable.datasourceName,
                    tableName = sourceTable.tableName
                )
                val destinationTableIsValid = validateTable(
                    datasourceName = destinationTable.datasourceName,
                    tableName = destinationTable.tableName
                )
                return sourceTableIsValid + destinationTableIsValid
            }

            abstract fun check(result: DbResult.Multiple<Int>): TestResult

            override fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult {
                val (rows: Pair<List<Int>, List<Int>>, executionTimeMillis: Long) = executeAndMeasureTimeMillis {
                    val sourceDatasource = datasourceImplementations[sourceTable.datasourceName]
                        ?: throw DatasourceNotFound(datasourceName = sourceTable.datasourceName)
                    val destinationDatasource =
                        datasourceImplementations[destinationTable.datasourceName] ?: throw DatasourceNotFound(
                            datasourceName = destinationTable.datasourceName
                        )
                    val sourceRows: List<Int> = serialTransaction(ds = sourceDatasource) {
                        execAndMap(sql = sourceSQL) { rs ->
                            rs.getInt(1)
                        }
                    }
                    val destinationRows: List<Int> = serialTransaction(ds = destinationDatasource) {
                        execAndMap(sql = destinationSQL) { rs ->
                            rs.getInt(1)
                        }
                    }
                    sourceRows to destinationRows
                }
                val result = DbResult.Multiple(
                    sourceValues = rows.first,
                    destinationValues = rows.second,
                    executionTimeMillis = executionTimeMillis)
                return check(result = result)
            }

            internal fun passed(executionTimeMillis: Long): TestResult.Passed.RowComparisonOK =
                TestResult.Passed.RowComparisonOK(
                    testDescription = description,
                    sourceDatasourceName = sourceTable.datasourceName,
                    sourceTableName = sourceTable.tableName,
                    sourceSql = sourceSQL,
                    destinationDatasourceName = destinationTable.datasourceName,
                    destinationTableName = destinationTable.tableName,
                    destinationSql = destinationSQL,
                    flex = flex,
                    flexPercent = flexPercent,
                    mostly = mostly,
                    executionTimeMillis = executionTimeMillis
                )

            @Serializable
            data class RowsMatch(
                override val sourceTable: Table,
                override val destinationTable: Table,
                override val flex: Float,
                override val flexPercent: Float
            ) : Rows() {

                override val mostly: Float = 1f

                override val description =
                    "${sourceTable.tableName} rows should match ${destinationTable.tableName} rows."

                override fun check(result: DbResult.Multiple<Int>): TestResult {
                    return if (result.sourceValues == result.destinationValues) {
                        super.passed(executionTimeMillis = result.executionTimeMillis)
                    } else {
                        TestResult.Failed.RowCounts.DoNotMatch(
                            testDescription = description,
                            sourceDatasourceName = sourceTable.datasourceName,
                            sourceTableName = sourceTable.tableName,
                            sourceSql = sourceSQL,
                            destinationDatasourceName = destinationTable.datasourceName,
                            destinationTableName = destinationTable.tableName,
                            destinationSql = destinationSQL,
                            sourceRows = result.sourceValues.first(),
                            destinationRows = result.destinationValues.first(),
                            flex = flex,
                            flexPercent = flexPercent,
                            mostly = mostly,
                            executionTimeMillis = result.executionTimeMillis
                        )
                    }
                }
            }
        }

        @Serializable
        sealed class Columns<V> : MultiTable() {
            abstract val sourceField: Field
            abstract val destinationField: Field
            abstract val maxFalsifyingExamples: Int

            abstract fun check(result: DbResult.Multiple<V>): TestResult

            override fun validate(): RuleValidationResult {
                val sourceColumnIsValid = validateColumn(
                    datasourceName = sourceField.datasourceName,
                    tableName = sourceField.tableName,
                    fieldName = sourceField.fieldName,
                    maxFalsifyingExamples = sourceField.maxFalsifyingExamples
                )
                val destinationColumnIsValid = validateColumn(
                    datasourceName = destinationField.datasourceName,
                    tableName = destinationField.tableName,
                    fieldName = destinationField.fieldName,
                    maxFalsifyingExamples = destinationField.maxFalsifyingExamples
                )
                return sourceColumnIsValid + destinationColumnIsValid
            }

            internal fun passed(executionTimeMillis: Long): TestResult.Passed.ValueComparisonOK =
                TestResult.Passed.ValueComparisonOK(
                    testDescription = description,
                    sourceDatasourceName = sourceField.datasourceName,
                    sourceTableName = sourceField.tableName,
                    sourceFieldName = sourceField.fieldName,
                    sourceSql = sourceSQL,
                    destinationDatasourceName = destinationField.datasourceName,
                    destinationTableName = destinationField.tableName,
                    destinationFieldName = destinationField.fieldName,
                    destinationSql = destinationSQL,
                    flex = flex,
                    flexPercent = flexPercent,
                    mostly = mostly,
                    executionTimeMillis = executionTimeMillis
                )

            @Serializable
            sealed class Numbers<N> : Columns<N>()
                where N : Number, N : Comparable<N> {

                @Serializable
                data class ShouldMatch<N>(
//                    @ContextualSerialization
                    override val sourceField: Field,
                    override val destinationField: Field,
                    override val maxFalsifyingExamples: Int,
                    override val flex: Float,
                    override val flexPercent: Float
                ) : Numbers<N>() where N : Number, N : Comparable<N> {

                    override val mostly: Float = 1f

                    override val description: String = "Column totals should match."

                    override val sourceSQL: String by lazy {
                        "SELECT SUM(${wrapField(fieldName = sourceField.fieldName, dataType = sourceField.dataType, dialect = sourceField.dialect)}) AS total " +
                            "FROM ${tableOrSubquery(
                                tableName = sourceField.tableName,
                                subquery = sourceField.subquery
                            )}"
                    }

                    override val destinationSQL: String by lazy {
                        "SELECT SUM(${wrapField(fieldName = destinationField.fieldName, dataType = destinationField.dataType, dialect = destinationField.dialect)}) AS total " +
                            "FROM ${tableOrSubquery(
                                tableName = destinationField.tableName,
                                subquery = destinationField.subquery
                            )}"
                    }

                    override fun evaluate(datasourceImplementations: Map<String, DataSource>): TestResult {
                        val (results: Pair<List<N>, List<N>>, executionTimeMillis: Long) = executeAndMeasureTimeMillis {
                            val sourceDatasource = datasourceImplementations[sourceField.datasourceName]
                                ?: throw DatasourceNotFound(datasourceName = sourceField.datasourceName)
                            val destinationDatasource =
                                datasourceImplementations[destinationField.datasourceName] ?: throw DatasourceNotFound(
                                    datasourceName = destinationField.datasourceName
                                )
                            val sourceResult: List<N> = sourceDatasource.getFirstColumnValues(
                                sql = sourceSQL,
                                dataType = sourceField.dataType
                            )
                            val destinationResult: List<N> = destinationDatasource.getFirstColumnValues(
                                sql = destinationSQL,
                                dataType = destinationField.dataType
                            )
                            sourceResult to destinationResult
                        }
                        val result = DbResult.Multiple(
                            sourceValues = results.first,
                            destinationValues = results.second,
                            executionTimeMillis = executionTimeMillis
                        )
                        return check(result = result)
                    }

                    override fun check(result: DbResult.Multiple<N>): TestResult {
                        return if (result.sourceValues == result.destinationValues) {
                            super.passed(result.executionTimeMillis)
                        } else {
                            TestResult.Failed.ValuesDontMatch.TotalsDontMatch(
                                testDescription = description,
                                sourceDatasourceName = sourceField.datasourceName,
                                sourceTableName = sourceField.tableName,
                                sourceFieldName = sourceField.fieldName,
                                sourceSql = sourceSQL,
                                destinationDatasourceName = destinationField.datasourceName,
                                destinationTableName = destinationField.tableName,
                                destinationFieldName = destinationField.fieldName,
                                destinationSql = destinationSQL,
                                flex = flex,
                                flexPercent = flexPercent,
                                mostly = mostly,
                                executionTimeMillis = result.executionTimeMillis,
                                sourceTotal = result.sourceValues.first(),
                                destinationTotal = result.destinationValues.first()
                            )
                        }
                    }
                }
            }
        }
    }
}
