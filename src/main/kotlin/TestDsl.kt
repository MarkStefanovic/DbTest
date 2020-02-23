import kotlinx.serialization.Serializable
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import javax.sql.DataSource


@DslMarker
annotation class TestDsl

@Serializable
data class TestSuite(
    val datasources: List<Datasource>,
    private val multiTableRules: List<Rule.MultiTable>
) {
    val rules: List<Rule> by lazy {
        val tempRules = mutableListOf<Rule>()
        datasources.forEach { ds ->
            tempRules.addAll(multiTableRules)
            ds.tables.forEach { tbl ->
                tempRules.addAll(tbl.rules)
                tempRules.addAll(tbl.fields.flatMap { it.rules })
            }
        }
        tempRules
    }

    fun validateRules(): RuleValidationResult =
        rules.fold(IsValid) { status: RuleValidationResult, rule: Rule ->
            status + rule.validate()
        }


    fun runTests(datasourceImplementations: Map<String, DataSource>): List<TestResult> =
        rules.map { rule: Rule ->
            rule.evaluate(datasourceImplementations = datasourceImplementations)
        }

    operator fun get(datasourceName: String): Datasource? =
        datasources.firstOrNull { it.name == datasourceName }

    @TestDsl
    class Builder {
        var maxFalsifyingExamples: Int = 3
        private val datasources: MutableList<Datasource> = mutableListOf()
        private val rules: MutableList<Rule.MultiTable> = mutableListOf()

        fun datasource(
            name: String,
            dialect: SQLDialect,
            init: Datasource.Builder.() -> Unit
        ): Datasource {
            val ds = Datasource.Builder(
                datasourceName = name,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            datasources.add(ds)
            return ds
        }

        internal fun datasource(
            name: String,
            dialect: String,
            init: Datasource.Builder.() -> Unit
        ): Datasource {
            val sqlDialect: SQLDialect = when (dialect.trim().toLowerCase()) {
                "db2" -> SQLDialect.DB2
                "mysql" -> SQLDialect.MYSQL
                "mssql" -> SQLDialect.MSSQL
                "oracle" -> SQLDialect.ORACLE
                "postgres", "postgresql" -> SQLDialect.POSTGRES
                "sqlite" -> SQLDialect.SQLITE
                else -> throw IllegalArgumentException(
                    "$dialect is not a recognized dialect name.  Available options include 'db2', 'mssql', " +
                        "'oracle', 'postgres', and 'sqlite'."
                )
            }
            val ds = Datasource.Builder(
                datasourceName = name,
                dialect = sqlDialect,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            datasources.add(ds)
            return ds
        }

        fun sqliteDatasource(name: String, init: Datasource.Builder.() -> Unit): Datasource {
            val ds = Datasource.Builder(
                datasourceName = name,
                dialect = SQLDialect.SQLITE,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            datasources.add(ds)
            return ds
        }

        fun mssqlDatasource(name: String, init: Datasource.Builder.() -> Unit): Datasource {
            val ds = Datasource.Builder(
                datasourceName = name,
                dialect = SQLDialect.MSSQL,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            datasources.add(ds)
            return ds
        }

        fun tablesShould(init: RulesBuilder.() -> Unit): List<Rule> {
            val newRules = RulesBuilder(maxFalsifyingExamples = maxFalsifyingExamples)
                .apply(init)
                .build()
            rules.addAll(newRules)
            return newRules
        }

        fun build(): TestSuite = TestSuite(datasources = datasources, multiTableRules = rules)
    }

    @TestDsl
    class RulesBuilder(
        private val rules: MutableList<Rule.MultiTable> = mutableListOf(),
        private val maxFalsifyingExamples: Int
    ) {

        fun build(): List<Rule.MultiTable> = rules

        fun haveMatchingRows(
            sourceTable: Table,
            destinationTable: Table,
            flexRows: Int = 0,
            flexPercent: Float = 0f
        ): Rule.MultiTable.Rows.RowsMatch {
            val rule = Rule.MultiTable.Rows.RowsMatch(
                sourceTable = sourceTable,
                destinationTable = destinationTable,
                flex = flexRows.toFloat(),
                flexPercent = flexPercent
            )
            rules.add(rule)
            return rule
        }

        fun <N> haveMatchingTotals(
            sourceField: Field.Numbers<N>,
            destinationField: Field.Numbers<N>,
            flex: N? = null,
            flexPercent: Float = 0f
        ): Rule.MultiTable.Columns.Numbers.ShouldMatch<N>
            where N : Number, N : Comparable<N> {

            val flexAmt: Float = flex?.toFloat() ?: 0f

            val rule = Rule.MultiTable.Columns.Numbers.ShouldMatch<N>(
                sourceField = sourceField,
                destinationField = destinationField,
                maxFalsifyingExamples = maxFalsifyingExamples,
                flex = flexAmt,
                flexPercent = flexPercent
            )
            rules.add(rule)
            return rule
        }
    }
}

@Serializable
data class Datasource(
    val name: String,
    val dialect: SQLDialect,
    val tables: List<Table>,
    val maxFalsifyingExamples: Int
) {
    operator fun get(tableName: String): Table =
        tables.firstOrNull { it.tableName == tableName }
            ?: error("A table named '$tableName' was not found in the $name datasource.")

    @TestDsl
    class Builder(
        val datasourceName: String,
        val dialect: SQLDialect,
        val maxFalsifyingExamples: Int
    ) {
        private val tables: MutableList<Table> = mutableListOf()

        fun table(name: String, init: Table.Builder.() -> Unit): Table {
            val tbl: Table = Table.Builder(
                datasourceName = datasourceName,
                tableName = name,
                subquery = null,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            tables.add(tbl)
            return tbl
        }

        fun subquery(name: String, sql: String, init: Table.Builder.() -> Unit): Table {
            val qry: Table = Table.Builder(
                datasourceName = datasourceName,
                tableName = name,
                subquery = sql,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            tables.add(qry)
            return qry
        }

        fun build(): Datasource = Datasource(
            name = datasourceName,
            tables = tables,
            dialect = dialect,
            maxFalsifyingExamples = maxFalsifyingExamples
        )


    }
}

@Serializable
data class Table(
    val datasourceName: String,
    val tableName: String,
    val fields: List<Field>,
    val rules: List<Rule.SingleTable.Rows>,
    val subquery: String?,
    val dialect: SQLDialect,
    val maxFalsifyingExamples: Int
) {
    operator fun get(fieldName: String): Field =
        fields.firstOrNull { it.fieldName == fieldName }
            ?: error("A field named '$fieldName' was not found on the $tableName table.")

    @Suppress("UNCHECKED_CAST")
    fun dateField(fieldName: String): Field.Dates<LocalDate> =
        get(fieldName = fieldName) as? Field.Dates<LocalDate>
            ?: error("A field named '$fieldName' was found, but it is not a date field.")

    @Suppress("UNCHECKED_CAST")
    fun dateTimeField(fieldName: String): Field.Dates<LocalDateTime> =
        get(fieldName = fieldName) as? Field.Dates<LocalDateTime>
            ?: error("A field named '$fieldName' was found, but it is not a dateTime field.")

    @Suppress("UNCHECKED_CAST")
    fun decimalField(fieldName: String): Field.Numbers<BigDecimal> =
        get(fieldName = fieldName) as? Field.Numbers<BigDecimal>
            ?: error("A field named '$fieldName' was found, but it is not a decimal field.")

    @Suppress("UNCHECKED_CAST")
    fun floatField(fieldName: String): Field.Numbers<Float> =
        get(fieldName = fieldName) as? Field.Numbers<Float>
            ?: error("A field named '$fieldName' was found, but it is not a float field.")

    @Suppress("UNCHECKED_CAST")
    fun intField(fieldName: String): Field.Numbers<Int> =
        get(fieldName = fieldName) as? Field.Numbers<Int>
            ?: error("A field named '$fieldName' was found, but it is not an int field.")

    @Suppress("UNCHECKED_CAST")
    fun textField(fieldName: String): Field.Text =
        get(fieldName = fieldName) as? Field.Text
            ?: error("A field named '$fieldName' was found, but it is not a text field.")


    @TestDsl
    class Builder(
        private val datasourceName: String,
        private val tableName: String,
        private val subquery: String?,
        private val dialect: SQLDialect,
        private val maxFalsifyingExamples: Int
    ) {
        private val fields = mutableListOf<Field>()
        private val rowRules = mutableListOf<Rule.SingleTable.Rows>()

        fun build(): Table = Table(
            datasourceName = datasourceName,
            tableName = tableName,
            subquery = subquery,
            fields = fields,
            rules = rowRules,
            dialect = dialect,
            maxFalsifyingExamples = maxFalsifyingExamples
        )

        fun fields(init: FieldRulesBuilder.() -> Unit = {}): List<Field> {
            val newRules = FieldRulesBuilder(
                datasourceName = datasourceName,
                tableName = tableName,
                subquery = subquery,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples
            ).apply(init).build()
            fields.addAll(newRules)
            return newRules
        }


        fun rows(init: RowsRuleBuilder.() -> Unit = {}): List<Rule.SingleTable.Rows> {
            val newRules = RowsRuleBuilder(
                datasourceName = datasourceName,
                tableName = tableName,
                subquery = subquery,
                dialect = dialect
            ).apply(init).build()
            rowRules.addAll(newRules)
            return rowRules
        }


        @TestDsl
        class FieldRulesBuilder(
            private val datasourceName: String,
            private val tableName: String,
            private val subquery: String?,
            private val dialect: SQLDialect,
            private val maxFalsifyingExamples: Int
        ) {
            private val fields: MutableList<Field> = mutableListOf()

            fun build(): List<Field> = fields

            fun dateField(name: String, init: Field.Dates.Builder<LocalDate>.() -> Unit = {}): Field.Dates<LocalDate> {
                val field = Field.Dates.Builder<LocalDate>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = DataType.DATE,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }

            fun dateTimeField(
                name: String,
                init: Field.Dates.Builder<LocalDateTime>.() -> Unit = {}
            ): Field.Dates<LocalDateTime> {
                val field = Field.Dates.Builder<LocalDateTime>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = DataType.DATETIME,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }

            fun decimalField(
                name: String,
                init: Field.Numbers.Builder<BigDecimal>.() -> Unit = {}
            ): Field.Numbers<BigDecimal> {
                val field = Field.Numbers.Builder<BigDecimal>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = DataType.DECIMAL,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }

            fun floatField(
                name: String,
                init: Field.Numbers.Builder<Float>.() -> Unit = {}
            ): Field.Numbers<Float> {
                val field = Field.Numbers.Builder<Float>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = DataType.FLOAT,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }

            fun intField(
                name: String,
                init: Field.Numbers.Builder<Int>.() -> Unit = {}
            ): Field.Numbers<Int> {
                val field = Field.Numbers.Builder<Int>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = DataType.INTEGER,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }

            fun textField(
                name: String,
                caseSensitive: Boolean,
                init: Field.Text.Builder.() -> Unit = {}
            ): Field.Text {
                val field = Field.Text.Builder(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = name,
                    subquery = subquery,
                    dialect = dialect,
                    caseSensitive = caseSensitive,
                    maxFalsifyingExamples = maxFalsifyingExamples
                ).apply(init).build()
                fields.add(field)
                return field
            }
        }

        @TestDsl
        class RowsRuleBuilder(
            private val datasourceName: String,
            private val tableName: String,
            private val subquery: String?,
            private val dialect: SQLDialect,
            private val rules: MutableList<Rule.SingleTable.Rows> = mutableListOf()
        ) {
            fun build(): List<Rule.SingleTable.Rows> = rules

            fun shouldEqual(
                rows: Int,
                flexRows: Int = 0,
                flexPercent: Float = 0f
            ): Rule.SingleTable.Rows.ShouldEqual {
                val newRule = Rule.SingleTable.Rows.ShouldEqual(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    subquery = subquery,
                    rows = rows,
                    dialect = dialect,
                    flex = flexRows.toFloat(),
                    flexPercent = flexPercent
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeAtLeast(
                minRows: Int,
                flexRows: Int = 0,
                flexPercent: Float = 0f
            ): Rule.SingleTable.Rows.ShouldBeAtLeast {
                val newRule = Rule.SingleTable.Rows.ShouldBeAtLeast(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    subquery = subquery,
                    dialect = dialect,
                    minRows = minRows,
                    flex = flexRows.toFloat(),
                    flexPercent = flexPercent
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeAtMost(
                maxRows: Int,
                flexRows: Int = 0,
                flexPercent: Float = 0f
            ): Rule.SingleTable.Rows.ShouldBeAtMost {
                val newRule = Rule.SingleTable.Rows.ShouldBeAtMost(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    subquery = subquery,
                    dialect = dialect,
                    maxRows = maxRows,
                    flex = flexRows.toFloat(),
                    flexPercent = flexPercent
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeBetween(
                minRows: Int,
                maxRows: Int,
                flexRows: Int = 0,
                flexPercent: Float = 0f
            ): Rule.SingleTable.Rows.ShouldBeBetween {
                val newRule = Rule.SingleTable.Rows.ShouldBeBetween(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    subquery = subquery,
                    dialect = dialect,
                    minRows = minRows,
                    maxRows = maxRows,
                    flex = flexRows.toFloat(),
                    flexPercent = flexPercent
                )
                rules.add(newRule)
                return newRule
            }
        }
    }
}

@Serializable
sealed class Field {
    abstract val datasourceName: String
    abstract val tableName: String
    abstract val fieldName: String
    abstract val rules: List<Rule.SingleTable.Column<*>>
    abstract val subquery: String?
    abstract val dataType: DataType
    abstract val dialect: SQLDialect
    abstract val maxFalsifyingExamples: Int

    @Serializable
    data class Dates<D>(
        override val datasourceName: String,
        override val tableName: String,
        override val fieldName: String,
        override val rules: List<Rule.SingleTable.Column.Dates<D>>,
        override val subquery: String?,
        override val dialect: SQLDialect,
        override val dataType: DataType,
        override val maxFalsifyingExamples: Int
    ) : Field() {

        @TestDsl
        class Builder<D>(
            private val datasourceName: String,
            private val tableName: String,
            private val fieldName: String,
            private val subquery: String?,
            private val dialect: SQLDialect,
            private val dataType: DataType,
            private val maxFalsifyingExamples: Int
        ) {
            private val rules: MutableList<Rule.SingleTable.Column.Dates<D>> = mutableListOf()

            fun shouldBeAfter(
                date: LocalDateTime,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeAfter<D> {
                val newRule = Rule.SingleTable.Column.Dates.ShouldBeAfter<D>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = dataType,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    date = date,
                    flex = flexDays.toFloat(),
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeAfter(
                localDateStr: String,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeAfter<D> =
                shouldBeAfter(
                    date = localDateStr.toLocalDateTime(dataType = dataType),
                    flexDays = flexDays,
                    flexPercent = flexPercent,
                    mostly = mostly
                )

            fun shouldBeBefore(
                date: LocalDateTime,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeBefore<D> {
                val newRule = Rule.SingleTable.Column.Dates.ShouldBeBefore<D>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = dataType,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    date = date,
                    flex = flexDays.toFloat(),
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeBefore(
                localDateStr: String,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeBefore<D> =
                shouldBeBefore(
                    date = localDateStr.toLocalDateTime(dataType = dataType),
                    flexDays = flexDays,
                    flexPercent = flexPercent,
                    mostly = mostly
                )

            fun shouldBeBetween(
                minDate: LocalDateTime,
                maxDate: LocalDateTime,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeBetween<D> {
                val newRule = Rule.SingleTable.Column.Dates.ShouldBeBetween<D>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = dataType,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    minDate = minDate,
                    maxDate = maxDate,
                    flex = flexDays.toFloat(),
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeBetween(
                minLocalDateStr: String,
                maxLocalDateStr: String,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeBetween<D> =
                shouldBeBetween(
                    minDate = minLocalDateStr.toLocalDateTime(dataType = dataType),
                    maxDate = maxLocalDateStr.toLocalDateTime(dataType = dataType),
                    flexDays = flexDays,
                    flexPercent = flexPercent,
                    mostly = mostly
                )


            fun shouldBeOnOrAfter(
                date: LocalDateTime,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeOnOrAfter<D> {
                val newRule = Rule.SingleTable.Column.Dates.ShouldBeOnOrAfter<D>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = dataType,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    date = date,
                    flex = flexDays.toFloat(),
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeOnOrAfter(
                localDateStr: String,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeOnOrAfter<D> =
                shouldBeOnOrAfter(
                    date = localDateStr.toLocalDateTime(dataType = dataType),
                    flexDays = flexDays,
                    flexPercent = flexPercent,
                    mostly = mostly
                )

            fun shouldBeOnOrBefore(
                date: LocalDateTime,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeOnOrBefore<D> {
                val newRule = Rule.SingleTable.Column.Dates.ShouldBeOnOrBefore<D>(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    dataType = dataType,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    date = date,
                    flex = flexDays.toFloat(),
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeOnOrBefore(
                localDateStr: String,
                flexDays: Int = 0,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Dates.ShouldBeOnOrBefore<D>  =
                shouldBeOnOrBefore(
                    date = localDateStr.toLocalDateTime(dataType = dataType),
                    flexDays = flexDays,
                    flexPercent = flexPercent
                )

            fun build(): Dates<D> = Dates(
                datasourceName = datasourceName,
                tableName = tableName,
                fieldName = fieldName,
                rules = rules,
                subquery = subquery,
                dialect = dialect,
                dataType = dataType,
                maxFalsifyingExamples = maxFalsifyingExamples
            )
        }
    }

    @Serializable
    data class Numbers<N>(
        override val datasourceName: String,
        override val tableName: String,
        override val fieldName: String,
        override val subquery: String?,
        override val dialect: SQLDialect,
        override val maxFalsifyingExamples: Int,
        override val rules: List<Rule.SingleTable.Column.Numbers<N>>,
        override val dataType: DataType
    ) : Field()
        where N : Number, N : Comparable<N> {

        @TestDsl
        class Builder<N>(
            private val datasourceName: String,
            private val tableName: String,
            private val fieldName: String,
            private val subquery: String?,
            private val dialect: SQLDialect,
            private val maxFalsifyingExamples: Int,
            private val dataType: DataType
        ) where N : Number, N : Comparable<N> {

            private val rules: MutableList<Rule.SingleTable.Column.Numbers<N>> = mutableListOf()

            fun build() = Numbers<N>(
                datasourceName = datasourceName,
                tableName = tableName,
                fieldName = fieldName,
                subquery = subquery,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples,
                dataType = dataType,
                rules = rules
            )

            fun <V> shouldBeAtLeast(
                minValue: V,
                flex: V? = null,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Numbers.ShouldBeAtLeast<N>
                where V : Number, V : Comparable<V> {

                val flexAmt: Float = flex?.toFloat() ?: 0f

                val convertedValue: N = minValue.cast(dataType = dataType)

                val newRule = Rule.SingleTable.Column.Numbers.ShouldBeAtLeast(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    minValue = convertedValue,
                    dataType = dataType,
                    flex = flexAmt,
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun <V> shouldBeAtMost(
                maxValue: V,
                flex: V? = null,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Numbers.ShouldBeAtMost<N>
                where V : Number, V : Comparable<V> {

                val flexAmt: Float = flex?.toFloat() ?: 0f

                val convertedValue: N = maxValue.cast(dataType = dataType)

                val newRule = Rule.SingleTable.Column.Numbers.ShouldBeAtMost(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    maxValue = convertedValue,
                    dataType = dataType,
                    flex = flexAmt,
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun <V> shouldBeBetween(
                minValue: V,
                maxValue: V,
                flex: V? = null,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Numbers.ShouldBeBetween<N>
                where V : Number, V : Comparable<V> {

                val flexAmt: Float = flex?.toFloat() ?: 0f

                val convertedMinValue: N = minValue.cast(dataType = dataType)
                val convertedMaxValue: N = maxValue.cast(dataType = dataType)
                val newRule = Rule.SingleTable.Column.Numbers.ShouldBeBetween(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    minValue = convertedMinValue,
                    maxValue = convertedMaxValue,
                    dataType = dataType,
                    flex = flexAmt,
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun <V> shouldBeOneOf(
                vararg values: V,
                flex: V? = null,
                flexPercent: Float = 0f,
                mostly: Float = 1f
            ): Rule.SingleTable.Column.Numbers.ShouldBeOneOf<N>
                where V : Number, V : Comparable<V> {

                val flexAmt: Float = flex?.toFloat() ?: 0f
                val convertedValues: Set<N> = values.map { it.cast<N>(dataType = dataType) }.toSet()
                val newRule = Rule.SingleTable.Column.Numbers.ShouldBeOneOf(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    values = convertedValues,
                    dataType = dataType,
                    flex = flexAmt,
                    flexPercent = flexPercent,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }
        }
    }

    @Serializable
    data class Text(
        override val datasourceName: String,
        override val tableName: String,
        override val fieldName: String,
        override val subquery: String?,
        override val rules: List<Rule.SingleTable.Column.Text>,
        val caseSensitive: Boolean,
        override val dialect: SQLDialect,
        override val maxFalsifyingExamples: Int
    ) : Field() {

        override val dataType = DataType.TEXT

        @TestDsl
        class Builder(
            private val datasourceName: String,
            private val tableName: String,
            private val fieldName: String,
            private val subquery: String?,
            private val caseSensitive: Boolean,
            private val dialect: SQLDialect,
            private val maxFalsifyingExamples: Int
        ) {

            private val rules: MutableList<Rule.SingleTable.Column.Text> = mutableListOf()

            fun build(): Text = Text(
                datasourceName = datasourceName,
                tableName = tableName,
                fieldName = fieldName,
                subquery = subquery,
                dialect = dialect,
                maxFalsifyingExamples = maxFalsifyingExamples,
                caseSensitive = caseSensitive,
                rules = rules
            )

            fun shouldBeLike(fragment: String, mostly: Float = 1f):
                Rule.SingleTable.Column.Text.ShouldBeLike {
                val newRule = Rule.SingleTable.Column.Text.ShouldBeLike(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    fragment = fragment,
                    caseSensitive = caseSensitive,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    flex = 0f,
                    flexPercent = 0f,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldEndWith(suffix: String, mostly: Float = 1f):
                Rule.SingleTable.Column.Text.ShouldEndWith {

                val newRule = Rule.SingleTable.Column.Text.ShouldEndWith(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    suffix = suffix,
                    caseSensitive = caseSensitive,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    flex = 0f,
                    flexPercent = 0f,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldStartWith(prefix: String, mostly: Float = 1f):
                Rule.SingleTable.Column.Text.ShouldStartWith {

                val newRule = Rule.SingleTable.Column.Text.ShouldStartWith(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    prefix = prefix,
                    caseSensitive = caseSensitive,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    flex = 0f,
                    flexPercent = 0f,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }

            fun shouldBeOneOf(vararg values: String, mostly: Float = 1f):
                Rule.SingleTable.Column.Text.ShouldBeOneOf {

                val newRule = Rule.SingleTable.Column.Text.ShouldBeOneOf(
                    datasourceName = datasourceName,
                    tableName = tableName,
                    fieldName = fieldName,
                    subquery = subquery,
                    values = values.toSet(),
                    caseSensitive = caseSensitive,
                    dialect = dialect,
                    maxFalsifyingExamples = maxFalsifyingExamples,
                    flex = 0f,
                    flexPercent = 0f,
                    mostly = mostly
                )
                rules.add(newRule)
                return newRule
            }
        }
    }
}

/**
 * Convert a generic number type into a specific number type, as specified by a given [DataType].
 */
@Suppress("UNCHECKED_CAST")
private fun <N> Number.cast(dataType: DataType): N =
    when (dataType) {
        DataType.DATE, DataType.DATETIME, DataType.TEXT ->
            throw NotImplementedError(
                "The dataType for a number rule should be one of DataType.INTEGER, " +
                    "DataType.FLOAT, or DataType.DECIMAL, but $dataType was provided."
            )
        DataType.DECIMAL -> {
            try {
                BigDecimal.valueOf(this.toDouble()) as N
            } catch (e: Exception) {
                throw InvalidRuleException("The value $this could not be converted to a decimal.")
            }
        }
        DataType.FLOAT -> {
            try {
                this.toFloat() as N
            } catch (e: Exception) {
                throw InvalidRuleException("The value $this could not be converted to a float.")
            }
        }
        DataType.INTEGER -> {
            try {
                this.toInt() as N
            } catch (e: Exception) {
                throw InvalidRuleException("The value $this could not be converted to an integer.")
            }
        }
    }

private fun String.toLocalDateTime(dataType: DataType): LocalDateTime =
    when (dataType) {
        DataType.DATE -> {
            try {
                LocalDate.parse(this, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay() as LocalDateTime
            } catch (e: DateTimeParseException) {
                try {
                    LocalDateTime.parse(this, DateTimeFormatter.ISO_LOCAL_DATE_TIME) as LocalDateTime
                } catch (e: DateTimeParseException) {
                    throw IllegalArgumentException(
                        "Could not parse '$this', data type ${dataType.name} as a LocalDateTime using " +
                            "ISO_LOCAL_DATE or ISO_LOCAL_DATE_TIME."
                    )
                }
            }
        }
        DataType.DATETIME -> {
            try {
                LocalDateTime.parse(this, DateTimeFormatter.ISO_LOCAL_DATE_TIME) as LocalDateTime
            } catch (e: DateTimeParseException) {
                try {
                    LocalDate.parse(this, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay() as LocalDateTime
                } catch (e: DateTimeParseException) {
                    throw IllegalArgumentException(
                        "Could not parse '$this', data type ${dataType.name} as a LocalDateTime using " +
                            "ISO_LOCAL_DATE_TIME or ISO_LOCAL_DATE."
                    )
                }
            }
        }
        else -> throw IllegalArgumentException("Cannot parse $this as a ${dataType.name}.")
    }


fun testSuite(init: TestSuite.Builder.() -> Unit): TestSuite =
    TestSuite.Builder().apply(init).build()
