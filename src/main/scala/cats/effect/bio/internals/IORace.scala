package cats.effect.bio.internals

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.bio.internals.Callback.{Extensions, T => Callback}
import cats.effect.bio.BIO
import cats.effect.Fiber
import cats.effect.internals.Logger

import scala.concurrent.Promise

private[effect] object IORace {
  /**
    * Implementation for `BIO.race` - could be described with `racePair`,
    * but this way it is more efficient, as we no longer have to keep
    * internal promises.
    */
  def simple[E, A, B](lh: BIO[E, A], rh: BIO[E, B]): BIO[E, Either[A, B]] = {
    // Signals successful results
    def onSuccess[T, U](
                         isActive: AtomicBoolean,
                         other: IOConnection[E],
                         cb: Callback[E, Either[T, U]],
                         r: Either[T, U]): Unit = {

      if (isActive.getAndSet(false)) {
        // First interrupts the other task
        try other.cancel.unsafeRunAsyncAndForget()
        finally cb.async(Right(r))
      }
    }

    def onError[T](
                    active: AtomicBoolean,
                    cb: Callback.T[E, T],
                    other: IOConnection[E],
                    err: E): Unit = {

      if (active.getAndSet(false)) {
        try other.cancel.unsafeRunAsyncAndForget()
        finally cb.async(Left(err))
      } else {
        Logger.reportFailure(IORunLoop.CustomException(err))
      }
    }

    BIO.cancelable[E, Either[A, B]] { cb =>
      val active = new AtomicBoolean(true)
      // Cancelable connection for the left value
      val connL = IOConnection()
      // Cancelable connection for the right value
      val connR = IOConnection()

      // Starts concurrent execution for the left value
      IORunLoop.startCancelable[E, A](lh, connL, {
        case Right(a) =>
          onSuccess(active, connR, cb, Left(a))
        case Left(err) =>
          onError(active, cb, connR, err)
      })

      // Starts concurrent execution for the right value
      IORunLoop.startCancelable[E, B](rh, connR, {
        case Right(b) =>
          onSuccess(active, connL, cb, Right(b))
        case Left(err) =>
          onError(active, cb, connL, err)
      })

      // Composite cancelable that cancels both
      CancelUtils.cancelAll(connL.cancel, connR.cancel)
    }
  }

  /**
    * Implementation for `BIO.racePair`
    */
  def pair[E, A, B](lh: BIO[E, A], rh: BIO[E, B]): BIO[E, Either[(A, Fiber[BIO[E, ?], B]), (Fiber[BIO[E, ?], A], B)]] = {
    BIO.cancelable[E, Either[(A, Fiber[BIO[E, ?], B]), (Fiber[BIO[E, ?], A], B)]] { cb =>
      val active = new AtomicBoolean(true)
      // Cancelable connection for the left value
      val connL = IOConnection()
      val promiseL = Promise[Either[E, A]]()
      // Cancelable connection for the right value
      val connR = IOConnection()
      val promiseR = Promise[Either[E, B]]()

      // Starts concurrent execution for the left value
      IORunLoop.startCancelable[E, A](lh, connL, {
        case Right(a) =>
          if (active.getAndSet(false))
            cb.async(Right(Left((a, IOFiber.build[E, B](promiseR, connR)))))
          else
            promiseL.trySuccess(Right(a))

        case Left(err) =>
          if (active.getAndSet(false)) {
            cb.async(Left(err))
            connR.cancel.unsafeRunAsyncAndForget()
          } else {
            promiseL.trySuccess(Left(err))
          }
      })

      // Starts concurrent execution for the right value
      IORunLoop.startCancelable[E, B](rh, connR, {
        case Right(b) =>
          if (active.getAndSet(false))
            cb.async(Right(Right((IOFiber.build[E, A](promiseL, connL), b))))
          else
            promiseR.trySuccess(Right(b))

        case Left(err) =>
          if (active.getAndSet(false)) {
            cb.async(Left(err))
            connL.cancel.unsafeRunAsyncAndForget()
          } else {
            promiseR.trySuccess(Left(err))
          }
      })

      // Composite cancelable that cancels both
      CancelUtils.cancelAll(connL.cancel, connR.cancel)
    }
  }
}

