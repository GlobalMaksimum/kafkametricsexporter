import java.util.concurrent.Executors

import cats.effect.{Async, ContextShift, IO}
import org.scalatest.{FlatSpec, Matchers}
import cats.instances.list._
import cats.syntax.parallel._

import scala.concurrent.ExecutionContext

class IOMemoizeSpec extends FlatSpec with Matchers {
  def myfunc(a: String): String = {
    println(a)
    a
  }
  val ecTwo = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  implicit val csOne: ContextShift[IO] = IO.contextShift(ecTwo)
  "IO " should "memoize results" in {


    val memoized = Async.memoize(IO(myfunc("hello")))

    val xx = memoized.flatMap(a=>List.fill(100)(a).parSequence).unsafeRunSync()

  }

}
