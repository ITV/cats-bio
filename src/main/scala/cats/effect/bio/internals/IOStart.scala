package cats.effect.bio.internals

/*
 * Copyright (c) 2017-2019 The Typelevel Cats-effect Project Developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cats.effect.bio.BIO
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift, Fiber}

import scala.util.Right

private[effect] object IOStart {
  /**
    * Implementation for `IO.start`.
    */
  def apply[E, A](cs: ContextShift[BIO[E, ?]], fa: BIO[E, A])(implicit F: Concurrent[BIO[E, ?]]): BIO[E, Fiber[BIO[E, ?], A]] = {
    val start: Start[E, Fiber[BIO[E, ?], A]] = (_, cb) => {
      // Memoization
      val p = Deferred.unsafe[BIO[E, ?], Either[E, A]]

      // Starting the source `IO`, with a new connection, because its
      // cancellation is now decoupled from our current one
      val conn2 = IOConnection[E]()
      IORunLoop.startCancelable(IOForkedStart(fa, cs), conn2, (e: Either[E, A]) => p.complete(e).unsafeRunAsyncAndForget())

      cb(Right(fiber(p, conn2)))
    }
    BIO.Async(start, trampolineAfter = true)
  }

  private[internals] def fiber[E, A](p: Deferred[BIO[E, ?], Either[E, A]], conn: IOConnection[E]): Fiber[BIO[E, ?], A] = {
    Fiber[BIO[E, ?], A](p.get.flatMap(BIO.fromEither), conn.cancel)
  }
}