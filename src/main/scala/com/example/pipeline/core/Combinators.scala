package com.example.pipeline.core

/** Functional combinators and small utilities with no I/O.
 *
 * This module provides (per course spec: “Compose functions using combinators”):
 *  - `PipeOps#pipe` – readable function piping / composition.
 *  - `composeAll`   – compose a list of endomorphic functions.
 *  - `where`        – a readable filtering combinator.
 *  - `mapWhere`     – conditionally map elements while preserving others.
 *
 * All functions are pure and side-effect free.
 */
object Combinators {

  /** Adds a `pipe` method to any value for readable left-to-right composition.
   *
   * {{{
   *   import com.example.pipeline.core.Combinators._
   *
   *   val x   = 3
   *   val inc = (n: Int) => n + 1
   *   val dbl = (n: Int) => n * 2
   *
   *   val y = x.pipe(inc).pipe(dbl) // 8
   * }}}
   *
   * @param self the underlying value; syntax sugar only
   * @tparam A   the value type
   */
  implicit final class PipeOps[A](private val self: A) extends AnyVal {

    /** Apply a function to `self`, enabling `v.pipe(f).pipe(g)` style composition.
     *
     * @param f function to apply
     * @tparam B result type
     * @return the result of `f(self)`
     */
    def pipe[B](f: A => B): B = {
      require(f != null, "function 'f' must not be null")
      f(self)
    }
  }

  /** Compose a list of endomorphisms (`A => A`) into a single function.
   *
   * Functions are applied left-to-right in the order they appear in the list.
   * For an empty list, the identity function is returned.
   *
   * {{{
   *   val fs = List[Int => Int](_ + 1, _ * 2, _ - 3)
   *   val f  = composeAll(fs)
   *   f(10)  // ((10 + 1) * 2) - 3 == 19
   * }}}
   *
   * @param fs list of endomorphisms to compose in order
   * @tparam A input/output type of the composed function
   * @return a single function equivalent to applying all `fs` in sequence
   */
  def composeAll[A](fs: List[A => A]): A => A = {
    require(fs != null, "fs must not be null")
    require(fs.forall(_ != null), "fs must not contain null functions")
    fs.foldLeft(identity[A](_)) { (acc, f) => acc.andThen(f) }
  }

  /** Conditionally apply a transformation when the predicate holds; otherwise return the input unchanged.
   *
   * Useful for building readable pipelines with inline conditions.
   *
   * {{{
   *   val onlyEvenDoubled = where[Int](_ % 2 == 0)(_ * 2)
   *   onlyEvenDoubled(3) // 3
   *   onlyEvenDoubled(4) // 8
   * }}}
   *
   * @param p predicate to decide whether to apply `f`
   * @param f transformation to apply when `p(a)` is true
   * @tparam A element type
   * @return a function `A => A` that applies `f` only when `p` holds
   */
  def where[A](p: A => Boolean)(f: A => A): A => A = {
    require(p != null, "predicate 'p' must not be null")
    require(f != null, "transformation 'f' must not be null")
    (a: A) => if (p(a)) f(a) else a
  }

  /** Map elements that match a predicate while preserving non-matching elements as `Left`.
   *
   * This is handy when you want to transform only a subset while keeping
   * the original values of the rest.
   *
   * {{{
   *   val xs = List(1, 2, 3, 4)
   *   val rs = mapWhere[Int, String](_ % 2 == 0)(i => s"even:$i")(xs)
   *   // rs: List(Left(1), Right("even:2"), Left(3), Right("even:4"))
   * }}}
   *
   * @param p  predicate selecting which elements to transform
   * @param f  mapping function applied to matching elements
   * @param xs input list
   * @tparam A input element type
   * @tparam B mapped element type
   * @return list of `Either[A, B]` preserving positions
   */
  def mapWhere[A, B](p: A => Boolean)(f: A => B)(xs: List[A]): List[Either[A, B]] = {
    require(p != null, "predicate 'p' must not be null")
    require(f != null, "mapping function 'f' must not be null")
    require(xs != null, "input list 'xs' must not be null")
    xs.map(a => if (p(a)) Right(f(a)) else Left(a))
  }
}


