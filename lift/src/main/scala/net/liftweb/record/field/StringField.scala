/*
 * Copyright 2007-2008 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.liftweb.record.field

import scala.xml._
import net.liftweb.util._
import net.liftweb.http.{S, FieldError}
import S._

/**
 * A Field containing String content.
 */
abstract class StringField[OwnerType <: Record[OwnerType]](rec: OwnerType, maxLength: Int) extends Field[String, OwnerType] {

  def this(rec: OwnerType, maxLength: Int, value: String) = {
    this(rec, maxLength)
    set(value)
  }

  def this(rec: OwnerType, value: String) = {
    this(rec, -1)
    set(value)
  }

  def owner = rec

  override def setFromAny(in: Any): Can[String] = {
    in match {
      case seq: Seq[_] if !seq.isEmpty => seq.map(setFromAny)(0)
      case (s: String) :: _ => Full(this.set(s))
      case null => Full(this.set(null))
      case s: String => Full(this.set(s))
      case Some(s: String) => Full(this.set(s))
      case Full(s: String) => Full(this.set(s))
      case None | Empty | Failure(_, _, _) => Full(this.set(null))
      case o => Full(this.set(o.toString))
    }
  }

  def setFromString(s: String) : Can[SMyType] = Full(s)

  override def toForm = <input type="text" maxlength={maxLength.toString}
	 name={S.mapFunc(SFuncHolder(this.setFromAny(_)))} value={value match {case null => "" case s => s.toString}}/>

  override def defaultValue = ""

}


import java.sql.{ResultSet, Types}
import net.liftweb.mapper.{DriverType}

/**
 * A string field holding DB related logic
 */
abstract class DBStringField[OwnerType <: DBRecord[OwnerType]](rec: OwnerType, maxLength: Int) extends
  StringField[OwnerType](rec, maxLength) with JDBCField[String, OwnerType]{

  def this(rec: OwnerType, maxLength: Int, value: String) = {
    this(rec, maxLength)
    set(value)
  }

  def this(rec: OwnerType, value: String) = {
    this(rec, -1)
    set(value)
  }

  def targetSQLType = Types.VARCHAR

  /**
   * Given the driver type, return the string required to create the column in the database
   */
  def fieldCreatorString(dbType: DriverType, colName: String): String = colName+" VARCHAR("+maxLength+")"

  def jdbcFriendly(field : String) : String = value
}

trait TimeZoneField[OwnerType <: Record[OwnerType]] extends StringField[OwnerType] {
}

trait CountryField[OwnerType <: Record[OwnerType]] extends StringField[OwnerType] {
}

trait LocaleField[OwnerType <: Record[OwnerType]] extends StringField[OwnerType] {
}
