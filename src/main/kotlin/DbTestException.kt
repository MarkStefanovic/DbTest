import kotlin.Exception

open class DbTestException(message: String): Exception(message)

data class InvalidRuleException(override val message: String) : DbTestException(message)

data class DatasourceNotFound(val datasourceName: String): DbTestException(
    message = "A datasource implementation named '$datasourceName' was not provided."
)

data class InvalidDataTypeSpecification(val dataType: DataType): DbTestException(
    message = "The rule has a type parameter <V> that does not match its dataType, $dataType."
)

object EmptyResult: DbTestException("The result set is empty.")