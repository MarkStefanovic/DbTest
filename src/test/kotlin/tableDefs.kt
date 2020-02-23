import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.`java-time`.date
import org.jetbrains.exposed.sql.`java-time`.datetime

object Customers : Table("customer") {
    val id = integer("id")
    val name = varchar("name", 80)
    val dateAdded = datetime("date_added")

    override val primaryKey = PrimaryKey(id)
}

object Items : Table("item") {
    val id = integer("id")
    val name = varchar("name", 80)
    val weight = float("weight")
    val price = decimal("price", 19, 2)

    override val primaryKey = PrimaryKey(id)
}

object Sales : Table("sale") {
    val id = integer("id")
    val salesDate = date("sales_date")
    val customerId = integer("customer_id")
    val itemId = integer("item_id")
    val quantitySold = integer("quantity_sold")

    override val primaryKey = PrimaryKey(id)
}