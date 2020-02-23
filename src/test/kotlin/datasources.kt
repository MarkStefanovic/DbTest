import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import javax.sql.DataSource

fun withTestDb(ds: DataSource, block: Transaction.() -> Unit) {
    serialTransaction(ds = ds) {
        SchemaUtils.drop(Customers, Items, Sales)
        SchemaUtils.create(Customers, Items, Sales)
        block()
    }
}

fun getMssqlDatasource(): DataSource {
    val config = HikariConfig().apply {
        dataSourceClassName = "com.microsoft.sqlserver.jdbc.SQLServerDataSource";
        jdbcUrl = "jdbc:sqlserver://localhost;databaseName=TestDB;"
        maximumPoolSize = 10;
        username = "sa"
        password = "pUgzer8zbv23iu"
    }
    config.minimumIdle = 3
    config.maximumPoolSize = 10
    return HikariDataSource(config)
}

fun getSqliteDatasource(): DataSource {
    val config = HikariConfig().apply {
        jdbcUrl = "jdbc:sqlite:file:test?mode=memory&cache=shared"
    }
    config.minimumIdle = 3
    config.maximumPoolSize = 10
    return HikariDataSource(config)
}