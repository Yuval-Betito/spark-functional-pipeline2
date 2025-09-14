package com.example.pipeline.core

/**
 * Closed algebraic data type (ADT) for parse/validation errors produced by the pure parsing layer.
 *
 * This hierarchy is immutable and serializable so it is safe to ship inside Spark tasks.
 */
sealed trait ParseError extends Serializable {

  /**
   * Human-readable message suitable for logs and tests.
   *
   * @return a concise, descriptive error message
   */
  def message: String
}

/**
 * Companion namespace for the concrete error cases.
 */
object ParseError {

  /**
   * Indicates that a transaction CSV row failed validation or basic structural checks.
   *
   * @param raw    the original CSV row text that failed to parse/validate
   * @param reason a short explanation describing the failure
   */
  final case class BadTransaction(raw: String, reason: String) extends ParseError {
    require(raw != null, "raw must not be null")
    require(reason != null && reason.nonEmpty, "reason must not be empty")

    /**
     * @return formatted message describing the invalid transaction row and reason
     */
    override def message: String =
      s"Transaction row '$raw' is invalid: $reason"
  }

  /**
   * Indicates that a product CSV row failed validation or basic structural checks.
   *
   * @param raw    the original CSV row text that failed to parse/validate
   * @param reason a short explanation describing the failure
   */
  final case class BadProduct(raw: String, reason: String) extends ParseError {
    require(raw != null, "raw must not be null")
    require(reason != null && reason.nonEmpty, "reason must not be empty")

    /**
     * @return formatted message describing the invalid product row and reason
     */
    override def message: String =
      s"Product row '$raw' is invalid: $reason"
  }

  /**
   * Indicates a required field was missing during parsing or validation.
   *
   * @param field the logical field name that was expected but not present
   */
  final case class MissingField(field: String) extends ParseError {
    require(field != null && field.nonEmpty, "field must not be empty")

    /**
     * @return formatted message naming the missing field
     */
    override def message: String =
      s"Missing required field: $field"
  }

  /**
   * Indicates a field expected to be numeric could not be parsed into a number.
   *
   * @param field the logical field name that should contain a numeric value
   * @param raw   the raw textual value that failed numeric parsing
   */
  final case class InvalidNumber(field: String, raw: String) extends ParseError {
    require(field != null && field.nonEmpty, "field must not be empty")
    require(raw != null, "raw must not be null")

    /**
     * @return formatted message including the field name and the offending raw value
     */
    override def message: String =
      s"Invalid numeric value for $field: '$raw'"
  }
}







