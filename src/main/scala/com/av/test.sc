import cats._
import cats.implicits._
import cats.syntax.semigroup._
import cats.Monoid
import cats.instances.string._ // for Monoid

case class X(a: Int, b: Int, c: Int)

trait CustomClassSemigroupImpl extends Semigroup[X] {
  def combine(first: X, second: X): X = X(first.a )
}

Semigroup[X].combine(X(1,2,3), X(2,3,4))