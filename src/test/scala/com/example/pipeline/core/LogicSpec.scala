package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.example.pipeline.core.ParseError._

/**
 * Unit tests for the pure functional logic layer (parsing, numeric helpers, and aggregation).
 *
 * Covered:
 *  - CSV parsing to case classes with functional error handling (Either[ParseError, A]).
 *  - Tail-recursive helpers: sumTailRec / meanTailRec.
 *  - Aggregation: revenueByCategory on (Transaction, Product) pairs.
 *  - Currying: minTotalFilter / totalAtLeast.
 */
final class LogicSpec extends AnyFunSuite with Matchers {

  test("parseTransactionCsv parses valid line") {
    // arrange
    val line = "t1,u1,p1,2,10.5,1711300000000"

    // act + assert
    Logic.parseTransactionCsv(line) match {
      case Right(t) =>
        t.txnId     shouldBe "t1"
        t.quantity  shouldBe 2
        t.unitPrice shouldBe 10.5 +- 1e-9
      case Left(err) =>
        fail(s"Expected Right, got Left($err)")
    }
  }

  test("parseTransactionCsv fails on bad quantity") {
    // arrange
    val line = "t1,u1,p1,ZZ,10.5,1711300000000"

    // act + assert
    Logic.parseTransactionCsv(line) match {
      case Left(_: BadTransaction) => succeed
      case other                   => fail(s"Expected Left(BadTransaction), got $other")
    }
  }

  test("parseProductCsv parses valid line") {
    // arrange
    val line = "p1,Books"

    // act + assert
    Logic.parseProductCsv(line) match {
      case Right(p) =>
        p.productId shouldBe "p1"
        p.category  shouldBe "Books"
      case Left(err) =>
        fail(s"Expected Right, got Left($err)")
    }
  }

  test("sumTailRec and meanTailRec work") {
    // act + assert
    Logic.sumTailRec(List(1.0, 2.0, 3.0)) shouldBe 6.0
    Logic.meanTailRec(List(1.0, 2.0, 3.0)) shouldBe Some(2.0)
    Logic.meanTailRec(Nil)                  shouldBe None
  }

  test("revenueByCategory groups correctly") {
    // arrange
    val pBooks = Product("p1", "Books")
    val pToys  = Product("p2", "Toys")
    val t1 = Transaction("t1", "u1", "p1", 2, 10.0, 1L) // 20
    val t2 = Transaction("t2", "u2", "p2", 1, 50.0, 1L) // 50
    val t3 = Transaction("t3", "u3", "p1", 3,  5.0, 1L) // 15

    // act
    val revenueMap = Logic.revenueByCategory(List((t1, pBooks), (t2, pToys), (t3, pBooks)))

    // assert
    revenueMap("Books") shouldBe 35.0
    revenueMap("Toys")  shouldBe 50.0
  }

  // ===== Currying demo (additive tests only; no changes to existing ones) =====

  test("minTotalFilter is curried and works") {
    // arrange: eta-expansion via underscore gives Transaction => Boolean
    val f = Logic.minTotalFilter(10.0) _

    // assert
    f(Transaction("tA","u","p",2,5.0,0L))  shouldBe true   // 2*5.0 = 10.0
    f(Transaction("tB","u","p",1,9.99,0L)) shouldBe false
  }

  test("totalAtLeast is curried and works (alias)") {
    // arrange: with an expected type, no underscore needed
    val g: Transaction => Boolean = Logic.totalAtLeast(10.0)

    // assert
    g(Transaction("tC","u","p",2,5.0,0L))  shouldBe true
    g(Transaction("tD","u","p",1,9.99,0L)) shouldBe false

    // arrange: underscore for eta-expansion
    val h = Logic.totalAtLeast(10.0) _

    // assert
    h(Transaction("tE","u","p",2,5.0,0L))  shouldBe true
    h(Transaction("tF","u","p",1,9.99,0L)) shouldBe false
  }
}





