import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

class JsonSerializationTest: StringSpec() {
    val suite = testSuite {
        datasource(name = "dw", dialect = "sqlite") {
            table("customer") {
                rows {
                    shouldEqual(4)
                    shouldBeBetween(1, 3)
                }
                fields {
                    textField("name", caseSensitive = false) {
                        shouldBeLike("ar")
                        shouldStartWith("M")
                    }
                }
            }
        }
    }

    @UnstableDefault val json = Json(JsonConfiguration.Default)

    init {
        "Field is serializable" {
            suite["dw"]?.get("customer")?.get("name")?.let {
                val jsonStr = json.stringify(Field.serializer(), it)
                val obj = json.parse(Field.serializer(), jsonStr)
                obj shouldBe it
            }
        }

        "Table is serialiable" {
            suite["dw"]?.get("customer")?.let {
                val jsonStr = json.stringify(Table.serializer(), it)
                val obj = json.parse(Table.serializer(), jsonStr)
                obj shouldBe it
            }

        }

        "Datasource is serializable" {
            suite["dw"]?.let {
                val jsonStr = json.stringify(Datasource.serializer(), it)
                val obj = json.parse(Datasource.serializer(), jsonStr)
                obj shouldBe it
            }
        }

        "TestSuite is serializable" {
            val jsonStr = json.stringify(TestSuite.serializer(), suite)
            val obj = json.parse(TestSuite.serializer(), jsonStr)
            obj shouldBe suite
        }
    }
}