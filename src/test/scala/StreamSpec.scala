import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import fs2.Stream
import scala.concurrent.duration._
class StreamSpec extends FlatSpec with Matchers {

  implicit val executionContext = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)


  it should "run once an eval" in  {
    Stream.eval(IO{
      println("here it is")
    }).flatMap{_=>
      Stream.fixedDelay(2 seconds)
    }.compile.drain.unsafeRunSync()
  }
}
