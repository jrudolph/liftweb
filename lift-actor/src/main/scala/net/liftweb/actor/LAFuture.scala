/*
 * Copyright 2009 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package net.liftweb.actor

import util._
import Helpers._

class LAFuture[T] {
  private var item: T = _
  private var satisfied = false

  def satisfy(value: T): Unit = synchronized {
    if (!satisfied) {
      item = value
      satisfied = true
    }
    notifyAll()
  }

  def get: T = synchronized {
    if (satisfied) item
    else {
      this.wait()
      if (satisfied) item
      else get
    }
  }

  def get(timeout: TimeSpan): Box[T] = synchronized {
    if (satisfied) Full(item)
    else {
      try {
        wait(timeout.millis)
        if (satisfied) Full(item)
        else Empty
      } catch {
        case _: InterruptedException => Empty
      }
    }
  }
}