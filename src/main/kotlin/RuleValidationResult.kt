sealed class RuleValidationResult

object IsValid: RuleValidationResult()

data class IsInvalid(val validationErrors: List<String>): RuleValidationResult()

operator fun RuleValidationResult.plus(other: RuleValidationResult): RuleValidationResult =
    when (this) {
        IsValid -> when (other) {
            IsValid -> this
            is IsInvalid -> other
        }
        is IsInvalid -> when (other) {
            IsValid -> this
            is IsInvalid -> IsInvalid(validationErrors = this.validationErrors + other.validationErrors)
        }
    }

fun RuleValidationResult.and(errorMessage: String, falsificationPredicate: () -> Boolean): RuleValidationResult {
    val failed = falsificationPredicate()
    val result = if (failed)
        IsInvalid(listOf(errorMessage))
    else
        IsValid
    return this + result
}