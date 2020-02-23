import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec


class DbUtilTests : StringSpec() {
    val ds = getMssqlDatasource()

    init {
        "test exacAndMap" {
            withTestDb(ds = ds) {
                exec(
                    """
                    INSERT INTO customer (id, name, date_added) 
                    VALUES 
                        (1, 'Mark', '2020-01-02'),
                        (2, 'Steve', '2020-02-01T03:12:02.321'),
                        (3, 'Mary', '2020-03-01T04:22:01.333'),
                        (4, 'Bill', '2020-04-01T04:22:01.333')
                """
                )
                val dbCustomers = execAndMap(sql = "SELECT * FROM customer") {
                    it.getString("name")
                }
                dbCustomers shouldBe listOf("Mark", "Steve", "Mary", "Bill")
            }
        }
    }
}