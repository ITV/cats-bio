package cats.effect.bio.internals

import cats.effect.Fiber
import cats.effect.bio.BIO
import cats.effect.internals.TrampolineEC.immediate

import scala.concurrent.{ExecutionContext, Promise}

private[effect] object IOStart {
  /**
    * Implementation for `IO.start`.
    */
  def apply[E, A](fa: BIO[E, A]): BIO[E, Fiber[BIO[E, ?], A]] =
    BIO.Async[E, Fiber[BIO[E, ?], A]] { (_, cb) =>
      implicit val ec: ExecutionContext = immediate
      // Light async boundary
      ec.execute(new Runnable {
        def run(): Unit = {
          // Memoization
          val p = Promise[Either[E, A]]()

          // Starting the source `IO`, with a new connection, because its
          // cancellation is now decoupled from our current one
          val conn2 = IOConnection()
          IORunLoop.startCancelable(fa, conn2, p.success)

          // Building a memoized IO - note we cannot use `IO.fromFuture`
          // because we need to link this `IO`'s cancellation with that
          // of the executing task
          val fiber = IOFiber.build(p, conn2)
          // Signal the newly created fiber
          cb(Right(fiber))
        }
      })
    }
}

/**
  * INTERNAL API - [[Fiber]] instantiated for [[BIO]].
  *
  * Not exposed, the `BIO` implementation exposes [[Fiber]] directly.
  */
final case class IOFiber[E, A](join: BIO[E, A])
  extends Fiber[BIO[E, ?], A] {

  def cancel: BIO[E, Unit] =
    IOCancel.signal(join)
}

object IOFiber {
  /** Trampolined execution context. */
  private[this] val ec: ExecutionContext = immediate
  /** Internal API */
  def build[E, A](p: Promise[Either[E, A]], conn: IOConnection[E]): Fiber[BIO[E, ?], A] =
    IOFiber(BIO.Async[E, A] { (ctx, cb) =>
      // Short-circuit for already completed `Future`
      p.future.value match {
        case Some(value) =>
          cb(value.get)
        case None =>
          // Cancellation needs to be linked to the active task
          ctx.push(conn.cancel)
          p.future.onComplete { r =>
            ctx.pop()
            cb(r.get)
          }(ec)
      }
    })
}
