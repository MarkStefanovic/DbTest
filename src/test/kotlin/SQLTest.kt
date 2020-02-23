import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

class SQLTest : StringSpec() {
    init {
        "generateWhereClause generates valid where clauses for MSSQL" {
            val rule = Rule.SingleTable.Column.Text.LengthsShouldBeBetween(
                datasourceName = "dw",
                subquery = null,
                dialect = SQLDialect.SQLITE,
                tableName = "dummy_table",
                fieldName = "dummy_field",
                maxFalsifyingExamples = 3,
                minLength = 3,
                maxLength = 40,
                flex = 0f,
                flexPercent = 0f,
                mostly = 1f
            )
            rule.predicateToFalsify shouldBe "LENGTH(\"dummy_field\") NOT BETWEEN 3 AND 40"
            val json = Json(JsonConfiguration.Default)
            val jsonData = json.stringify(Rule.SingleTable.Column.Text.LengthsShouldBeBetween.serializer(), rule)
            println(jsonData)

            val obj = json.parse(Rule.SingleTable.Column.Text.LengthsShouldBeBetween.serializer(), jsonData)
            println(obj)

        }
    }
}
