import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class RuleValidationTests: StringSpec() {
    init {
        "validate flags rules without a datasource name" {
            val rule = Rule.SingleTable.Rows.ShouldEqual(
                datasourceName = "",
                tableName = "customer_dim",
                subquery = null,
                dialect = SQLDialect.SQLITE,
                rows = 10,
                flex = 0f,
                flexPercent = 0f
            )
            rule.validate() shouldBe IsInvalid(validationErrors = listOf(
                "Datasource names cannot be blank."
            ))
        }

        "validate flags rules without a table name" {
            val rule = Rule.SingleTable.Rows.ShouldEqual(
                datasourceName = "dw",
                tableName = "",
                subquery = null,
                dialect = SQLDialect.SQLITE,
                rows = 10,
                flex = 0f,
                flexPercent = 0f
            )
            rule.validate() shouldBe IsInvalid(validationErrors = listOf(
                "Table names cannot be blank."
            ))
        }

        "validate flags rules without a field name" {
            val rule = Rule.SingleTable.Column.Text.ShouldStartWith(
                datasourceName = "dw",
                tableName = "customer",
                fieldName = "",
                subquery = null,
                dialect = SQLDialect.SQLITE,
                prefix = "test",
                caseSensitive = true,
                maxFalsifyingExamples = 3,
                flex = 0f,
                flexPercent = 0f,
                mostly = 1f
            )
            rule.validate() shouldBe IsInvalid(validationErrors = listOf(
                "Field names cannot be blank."
            ))
        }
    }
}