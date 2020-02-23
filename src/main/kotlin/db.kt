import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection
import java.sql.ResultSet
import javax.sql.DataSource

fun <T> Transaction.execAndMap(sql: String, transform: (ResultSet) -> T): List<T> {
    val result = arrayListOf<T>()
    this.exec(sql) { rs ->
        while (rs.next()) {
            result += transform(rs)
        }
    }
    return result
}

fun <T> serialTransaction(db: Database, statement: Transaction.() -> T): T =
    transaction(
        transactionIsolation = Connection.TRANSACTION_SERIALIZABLE,
        repetitionAttempts = 0,
        db = db,
        statement = statement
    )

fun <T> serialTransaction(ds: DataSource, statement: Transaction.() -> T): T {
    val db: Database = Database.connect(ds)
    return serialTransaction(db = db, statement = statement)
}
