package cats.effect.bio
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


import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import cats.effect.bio.internals.CancelUtils
import cats.effect.bio.internals.IORunLoop.CustomException
import org.scalatest.{FunSuite, Matchers}

import scala.util.control.NonFatal

class CancelUtilsTests extends FunSuite with Matchers with TestUtils {
  test("cancelAll works for zero references") {
    CancelUtils.cancelAll().unsafeRunSync()
  }

  test("cancelAll works for one reference") {
    var wasCanceled = false
    CancelUtils.cancelAll(BIO { wasCanceled = true }).unsafeRunSync()
    wasCanceled shouldBe true
  }

  test("cancelAll catches error from one reference") {
    val dummy = new RuntimeException("dummy")
    var wasCanceled1 = false
    var wasCanceled2 = false

    val io = CancelUtils.cancelAll(
      BIO { wasCanceled1 = true },
      BIO { throw dummy },
      BIO { wasCanceled2 = true }
    )

    try {
      io.unsafeRunSync()
      fail("should have throw exception")
    } catch {
      case CustomException(`dummy`) =>
        wasCanceled1 shouldBe true
        wasCanceled2 shouldBe true
    }
  }

  test("cancelAll catches the first error and logs the rest") {
    val dummy1 = new RuntimeException("dummy1")
    val dummy2 = new RuntimeException("dummy2")
    var wasCanceled1 = false
    var wasCanceled2 = false

    val io = CancelUtils.cancelAll(
      BIO { wasCanceled1 = true },
      BIO { throw dummy1 },
      BIO { throw dummy2 },
      BIO { wasCanceled2 = true }
    )

    val sysErr = new ByteArrayOutputStream()
    try {
      catchSystemErrInto(sysErr) {
        io.unsafeRunSync()
      }
      fail("should have throw exception")
    } catch {
      case NonFatal(error) =>
        error shouldBe CustomException(dummy1)
        sysErr.toString("utf-8") should include("dummy2")
        dummy1.getSuppressed shouldBe empty // ensure memory isn't leaked with addSuppressed
        dummy2.getSuppressed shouldBe empty // ensure memory isn't leaked with addSuppressed
    }
  }
}

trait TestUtils {

  /**
    * Silences `System.err`, only printing the output in case exceptions are
    * thrown by the executed `thunk`.
    */
  def silenceSystemErr[A](thunk: => A): A = synchronized {
    // Silencing System.err
    val oldErr = System.err
    val outStream = new ByteArrayOutputStream()
    val fakeErr = new PrintStream(outStream)
    System.setErr(fakeErr)
    try {
      val result = thunk
      System.setErr(oldErr)
      result
    } catch {
      case NonFatal(e) =>
        System.setErr(oldErr)
        // In case of errors, print whatever was caught
        fakeErr.close()
        val out = new String(outStream.toByteArray, StandardCharsets.UTF_8)
        if (out.nonEmpty) oldErr.println(out)
        throw e
    }
  }

  /**
    * Catches `System.err` output, for testing purposes.
    */
  def catchSystemErr(thunk: => Unit): String = {
    val outStream = new ByteArrayOutputStream()
    catchSystemErrInto(outStream)(thunk)
    new String(outStream.toByteArray, StandardCharsets.UTF_8)
  }

  /**
    * Catches `System.err` output into `outStream`, for testing purposes.
    */
  def catchSystemErrInto[T](outStream: OutputStream)(thunk: => T): T = synchronized {
    val oldErr = System.err
    val fakeErr = new PrintStream(outStream)
    System.setErr(fakeErr)
    try {
      thunk
    } finally {
      System.setErr(oldErr)
      fakeErr.close()
    }
  }
}
