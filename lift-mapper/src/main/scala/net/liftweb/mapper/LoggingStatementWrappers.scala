/*
 * LoggingStatementWrappers.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package net.liftweb.mapper

import _root_.java.io.{InputStream,Reader}
import _root_.java.net.URL
//import _root_.java.sql.{Blob,Clob,Connection,Date,NClob,PreparedStatement,Statement,Time,Timestamp}
import _root_.java.sql.{Array => SqlArray, _}
import _root_.java.util.Calendar

import _root_.net.liftweb.util.Box

class LoggedStatement(underlying : Statement) extends Statement {
  protected var executedStatements = List[String]()

  private val StatementClazz = classOf[Statement]

  // These are from wrapper and are required
  def isWrapperFor (clazz : Class[_]) : Boolean = clazz match {
      case StatementClazz => true
      case _ => underlying.isWrapperFor(clazz)
  }

  def unwrap[T] (clazz : Class[T]) : T = clazz match {
      case StatementClazz => underlying.asInstanceOf[T]
      case _ => underlying.unwrap(clazz)
  }
  
  def addBatch (sql : String) {
      executedStatements ::= sql
      underlying.addBatch(sql)
  }
  def cancel () {
      executedStatements ::= "Cancelled Statement"
      underlying.cancel()
  }
  def clearBatch () {
      executedStatements ::= "Cleared Batch"
      underlying.clearBatch()
  }
  def clearWarnings () {
      executedStatements ::= "Cleared Warnings"
      underlying.clearWarnings()
  }
  def close () {
      executedStatements ::= "Closed Statement"
      underlying.close()
  }
  def execute (sql : String) : Boolean = {
      val ret = underlying.execute(sql)
      executedStatements ::= "\"%s\" : ret = %s".format(sql, ret)
      ret
  }
  def execute (sql : String, autoKeys : Int) : Boolean = {
      val ret = underlying.execute(sql, autoKeys)
      executedStatements ::= "Exec \"%s\", Auto-gen keys = %s : ret = %s".format(sql, StatementConstantDescriptions.genKeyDescriptions(autoKeys), ret)
      ret
  }
  def execute (sql : String, autoKeyColumns : Array[Int]) : Boolean = {
      val ret = underlying.execute(sql, autoKeyColumns)
      executedStatements ::= "Exec \"%s\", Auto-gen keys for columns %s : ret = %s".format(sql, autoKeyColumns.mkString(", "), ret)
      ret
  }
  def execute (sql : String, autoKeyColumns : Array[String]) : Boolean = {
      val ret = underlying.execute(sql, autoKeyColumns)
      executedStatements ::= "Exec \"%s\", Auto-gen keys for columns %s : ret = %s".format(sql, autoKeyColumns.mkString(", "), ret)
      ret
  }
  def executeBatch () : Array[Int] = {
      val ret = underlying.executeBatch()
      executedStatements ::= "Executed batch, counts = " + ret.mkString("(", ", ", ")")
      ret
  }
  def executeQuery (sql : String) : ResultSet = {
      val rs = underlying.executeQuery(sql)
      executedStatements ::= "Exec query \"%s\" : rs = %s".format(sql, rs)
      rs
  }
  def executeUpdate (sql : String) : Int = {
      val ret = underlying.executeUpdate(sql)
      executedStatements ::= "Exec update \"%s\" : updated %d rows".format(sql, ret)
      ret
  }
  def executeUpdate (sql : String, autoKeys : Int) : Int = {
      val ret = underlying.executeUpdate(sql, autoKeys)
      executedStatements ::= "Exec update \"%s\", Auto-gen keys = %s : updated %d rows".format(sql, StatementConstantDescriptions.genKeyDescriptions(autoKeys), ret)
      ret
  }
  def executeUpdate (sql : String, autoKeyColumns : Array[Int]) : Int = {
      val ret = underlying.executeUpdate(sql, autoKeyColumns)
      executedStatements ::= "Exec update \"%s\", Auto-gen keys for columns %s : updated %d rows".format(sql, autoKeyColumns.mkString(", "), ret)
      ret
  }
  def executeUpdate (sql : String, autoKeyColumns : Array[String]) : Int = {
      val ret = underlying.executeUpdate(sql, autoKeyColumns)
      executedStatements ::= "Exec update \"%s\", Auto-gen keys for columns %s : updated %d rows".format(sql, autoKeyColumns.mkString(", "), ret)
      ret
  }
  def getConnection () : Connection = {
      executedStatements ::= "Get underlying Connection"
      underlying.getConnection
  }
  def getFetchDirection () : Int = {
      val ret = underlying.getFetchDirection()
      executedStatements ::= "Get fetch direction : " + StatementConstantDescriptions.fetchDirDescriptions(ret)
      ret
  }
  def getFetchSize () : Int = {
      val size = underlying.getFetchSize()
      executedStatements ::= "Get fetch size : " + size
      size
  }
  def getGeneratedKeys () : ResultSet = {
      val rs = underlying.getGeneratedKeys()
      executedStatements ::= "Get generated keys : rs = " + rs
      rs
  }
  def getMaxFieldSize () : Int = {
      val ret = underlying.getMaxFieldSize()
      executedStatements ::= "Get max field size : " + ret
      ret
  }
  def getMaxRows () : Int = {
      val ret = underlying.getMaxRows()
      executedStatements ::= "Get max rows : " + ret
      ret
  }
  def getMoreResults () : Boolean = {
      val ret = underlying.getMoreResults()
      executedStatements ::= "Get more results : " + ret
      ret
  }
  def getMoreResults (current : Int) : Boolean = {
      val ret = underlying.getMoreResults(current)
      executedStatements ::= "Get more results (%s) : %s".format(StatementConstantDescriptions.getMoreResultsDescriptions(current), ret)
      ret
  }
  def getQueryTimeout () : Int = {
      val ret = underlying.getQueryTimeout()
      executedStatements ::= "Get query timeout : " + ret
      ret
  }
  def getResultSet () : ResultSet = {
      val rs = underlying.getResultSet()
      executedStatements ::= "Get result set : " + rs
      rs
  }
  def getResultSetConcurrency () : Int = {
      val ret = underlying.getResultSetConcurrency()
      executedStatements ::= "Get result set concurrency : " + StatementConstantDescriptions.resultSetConcurrencyDescs(ret)
      ret
  }
  def getResultSetHoldability () : Int = {
      val ret = underlying.getResultSetHoldability()
      executedStatements ::= "Get ResultSet holdability : " + StatementConstantDescriptions.resultSetHoldabilityDescs(ret)
      ret
  }
  def getResultSetType () : Int = {
      val ret = underlying.getResultSetType()
      executedStatements ::= "Get ResultSet type : " + StatementConstantDescriptions.resultSetTypeDescs(ret)
      ret
  }
  def getUpdateCount () : Int = {
      val ret = underlying.getUpdateCount()
      executedStatements ::= "Get update count" + ret
      ret
  }
  def getWarnings () : SQLWarning = {
      val ret = underlying.getWarnings()
      executedStatements ::= "Get SQL Warnings: " + Box.!!(ret).map(_.toString).openOr("None")
      ret
  }
  def isClosed () : Boolean = {
      val ret = underlying.isClosed()
      executedStatements ::= "Check isClosed : " + ret
      ret
  }
  def isPoolable () : Boolean = {
      val ret = underlying.isPoolable()
      executedStatements ::= "Check isPoolable : " + ret
      ret
  }
  def setCursorName (name : String) {
      underlying.setCursorName(name)
      executedStatements ::= "Set cursor name = %s" + name
  }
  def setEscapeProcessing (enable : Boolean) {
      underlying.setEscapeProcessing(enable)
      executedStatements ::= "Set escape processing = " + enable
  }
  def setFetchDirection (direction : Int) {
      underlying.setFetchDirection(direction)
      executedStatements ::= "Set fetch direction = " + StatementConstantDescriptions.fetchDirDescriptions(direction)
  }
  def setFetchSize (size : Int) {
      underlying.setFetchSize(size)
      executedStatements ::= "Set fetch size = " + size
  }
  def setMaxFieldSize (size : Int) {
      underlying.setMaxFieldSize(size)
      executedStatements ::= "Set max field size = " + size
  }
  def setMaxRows (count : Int) {
      underlying.setMaxRows(count)
      executedStatements ::= "Set max rows = " + count
  }
  def setPoolable (poolable : Boolean) {
      underlying.setPoolable(poolable)
      executedStatements ::= "Set poolable = " + poolable
  }
  def setQueryTimeout (timeout : Int) {
      underlying.setQueryTimeout(timeout)
      executedStatements ::= "Set query timeout = " + timeout
  }

  override def toString = executedStatements.reverse.mkString("\n")
}

class LoggedPreparedStatement (stmt : String, underlying : PreparedStatement) extends LoggedStatement(underlying) with PreparedStatement {
  private var paramMap = Map.empty[Int,Any]

  private val PreparedStatementClazz = classOf[PreparedStatement]

  // These are from wrapper and are required
  override def isWrapperFor (clazz : Class[_]) : Boolean = clazz match {
      case PreparedStatementClazz => true
      case _ => super.isWrapperFor(clazz) || underlying.isWrapperFor(clazz)
  }

  override def unwrap[T] (clazz : Class[T]) : T = clazz match {
      case PreparedStatementClazz => underlying.asInstanceOf[T]
      case _ => if (super.isWrapperFor(clazz)) super.unwrap(clazz) else underlying.unwrap(clazz)
  }

  // utility method to fill in params
  private def paramified : String = {
      def substitute (in : String, index : Int) : String = in.indexOf('?') match {
          case -1 => in
          case j => substitute(in.substring(0,j) + paramMap(index) + in.substring(j + 1), index + 1)
      }

      substitute(stmt, 1)
  }

  def addBatch () {
      underlying.addBatch()
      executedStatements ::= "Batching \"%s\"".format(paramified)
  }

  def clearParameters () {
      underlying.clearParameters()
      executedStatements ::= "Clear parameters"
      paramMap = Map.empty[Int,Any]
  }

  def execute () : Boolean = {
      val ret = underlying.execute()
      executedStatements ::= "Exec \"%s\" : %s".format(paramified, ret)
      ret
  }

  def executeQuery () : ResultSet = {
      val rs = underlying.executeQuery()
      executedStatements ::= "Exec query \"%s\" : %s".format(paramified, rs)
      rs
  }

  def executeUpdate () : Int = {
      val ret = underlying.executeUpdate()
      executedStatements ::= "Exec update \"%s\" : updated %d rows".format(paramified, ret)
      ret
  }

  def getMetaData () : ResultSetMetaData = {
      val ret = underlying.getMetaData()
      executedStatements ::= "Get metadata : " + ret
      ret
  }

  def getParameterMetaData() : ParameterMetaData = {
      val ret = underlying.getParameterMetaData()
      executedStatements ::= "Get param metadata : " + ret
      ret
  }

  def setArray (index : Int, x : SqlArray) {
      underlying.setArray(index, x)
      paramMap += index -> x
  }

  def setAsciiStream (index : Int, x : InputStream) {
      underlying.setAsciiStream(index, x)
      paramMap += index -> "(Ascii Stream: %s)".format(x)
  }

  def setAsciiStream (index : Int, x : InputStream, length : Int) {
      underlying.setAsciiStream(index, x, length)
      paramMap += index -> "(Ascii Stream: %s (%d bytes))".format(x, length)
  }

  def setAsciiStream (index : Int, x : InputStream, length : Long) {
      underlying.setAsciiStream(index, x, length)
      paramMap += index -> "(Ascii Stream: %s (%d bytes))".format(x, length)
  }

  def setBigDecimal (index : Int, x : java.math.BigDecimal) {
      underlying.setBigDecimal(index, x)
      paramMap += index -> x
  }

  def setBinaryStream (index : Int, x : InputStream) {
      underlying.setBinaryStream(index, x)
      paramMap += index -> "(Binary Stream: %s)".format(x)
  }

  def setBinaryStream (index : Int, x : InputStream, length : Int) {
      underlying.setBinaryStream(index, x, length)
      paramMap += index -> "(Binary Stream: %s (%d bytes))".format(x, length)
  }

  def setBinaryStream (index : Int, x : InputStream, length : Long) {
      underlying.setBinaryStream(index, x, length)
      paramMap += index -> "(Binary Stream: %s (%d bytes))".format(x, length)
  }

  def setBlob (index : Int, x : Blob) {
      underlying.setBlob(index, x)
      paramMap += index -> "(Blob : %s)".format(x)
  }

  def setBlob (index : Int, x : InputStream) {
      underlying.setBlob(index, x)
      paramMap += index -> "(Blob : %s)".format(x)
  }

  def setBlob (index : Int, x : InputStream, length : Long) {
      underlying.setBlob(index, x, length)
      paramMap += index -> "(Blob : %s (%d bytes))".format(x, length)
  }

  def setBoolean (index : Int, x : Boolean) {
      underlying.setBoolean(index, x)
      paramMap += index -> x
  }

  def setByte (index : Int, x : Byte) {
      underlying.setByte(index, x)
      paramMap += index -> x
  }

  def setBytes (index : Int, x : Array[Byte]) {
      underlying.setBytes(index, x)
      paramMap += index -> x
  }

  def setCharacterStream (index : Int, x : Reader) {
      underlying.setCharacterStream(index, x)
      paramMap += index -> "(Char stream : %s)".format(x)
  }

  def setCharacterStream (index : Int, x : Reader, length : Int) {
      underlying.setCharacterStream(index, x, length)
      paramMap += index -> "(Char stream : %s (%d bytes))".format(x, length)
  }

  def setCharacterStream (index : Int, x : Reader, length : Long) {
      underlying.setCharacterStream(index, x, length)
      paramMap += index -> "(Char stream : %s (%d bytes))".format(x, length)
  }

  def setClob (index : Int, x : Clob) {
      underlying.setClob(index, x)
      paramMap += index -> "(Clob : %s)".format(x)
  }

  def setClob (index : Int, x : Reader) {
      underlying.setClob(index, x)
      paramMap += index -> "(Clob : %s)".format(x)
  }

  def setClob (index : Int, x : Reader, length : Long) {
      underlying.setClob(index, x, length)
      paramMap += index -> "(Clob : %s (%d bytes))".format(x, length)
  }

  def setDate (index : Int, x : Date) {
      underlying.setDate(index, x)
      paramMap += index -> x
  }

  def setDate (index : Int, x : Date, cal : Calendar) {
      underlying.setDate(index, x, cal)
      paramMap += index -> (x + ":" + cal)
  }

  def setDouble (index : Int, x : Double) {
      underlying.setDouble(index, x)
      paramMap += index -> x
  }

  def setFloat (index : Int, x : Float) {
      underlying.setFloat(index, x)
      paramMap += index -> x
  }

  def setInt (index : Int, x : Int) {
      underlying.setInt(index, x)
      paramMap += index -> x
  }

  def setLong (index : Int, x : Long) {
      underlying.setLong(index, x)
      paramMap += index -> x
  }

  def setNCharacterStream (index : Int, x : Reader) {
      underlying.setNCharacterStream(index, x)
      paramMap += index -> "(NChar Stream : %s)".format(x)
  }

  def setNCharacterStream (index : Int, x : Reader, length : Long) {
      underlying.setNCharacterStream(index, x, length)
      paramMap += index -> "(NChar Stream : %s (%d bytes))".format(x, length)
  }

  def setNClob (index : Int, x : NClob) {
      underlying.setNClob(index, x)
      paramMap += index -> "(NClob : %s)".format(x)
  }

  def setNClob (index : Int, x : Reader) {
      underlying.setNClob(index, x)
      paramMap += index -> "(NClob : %s)".format(x)
  }

  def setNClob (index : Int, x : Reader, length : Long) {
      underlying.setNClob(index, x, length)
      paramMap += index -> "(NClob : %s (%d bytes))".format(x, length)
  }

  def setNString (index : Int, x : String) {
      underlying.setNString(index, x)
      paramMap += index -> x
  }

  def setNull (index : Int, sqlType : Int) {
      underlying.setNull(index, sqlType)
      paramMap += index -> "NULL"
  }

  def setNull (index : Int, sqlType : Int, typeName : String) {
      underlying.setNull(index, sqlType, typeName)
      paramMap += index -> "NULL"
  }

  def setObject (index : Int, x : Object) {
      underlying.setObject(index, x)
      paramMap += index -> x
  }

  def setObject (index : Int, x : Object, sqlType : Int) {
      underlying.setObject(index, x, sqlType)
      paramMap += index -> "%s (type %s)".format(x, sqlType)
  }

  def setObject (index : Int, x : Object, sqlType : Int, scale : Int) {
      underlying.setObject(index, x, sqlType, scale)
      paramMap += index -> "%s (type %d, scale %d)".format(x, sqlType, scale)
  }

  def setRef (index : Int, x : Ref) {
      underlying.setRef(index, x)
      paramMap += index -> x
  }

  def setRowId (index : Int, x : RowId) {
      underlying.setRowId(index, x)
      paramMap += index -> x
  }

  def setShort (index : Int, x : Short) {
      underlying.setShort(index, x)
      paramMap += index -> x
  }

  def setSQLXML (index : Int, x : SQLXML) {
      underlying.setSQLXML(index, x)
      paramMap += index -> x
  }

  def setString (index : Int, x : String) {
      underlying.setString(index, x)
      paramMap += index -> "\"%s\"".format(x)
  }

  def setTime (index : Int, x : Time) {
      underlying.setTime(index, x)
      paramMap += index -> x
  }

  def setTime (index : Int, x : Time, cal : Calendar) {
      underlying.setTime(index, x, cal)
      paramMap += index -> (x + ":" + cal)
  }

  def setTimestamp (index : Int, x : Timestamp) {
      underlying.setTimestamp(index, x)
      paramMap += index -> x
  }

  def setTimestamp (index : Int, x : Timestamp, cal : Calendar) {
      underlying.setTimestamp(index, x, cal)
      paramMap += index -> (x + ":" + cal)
  }

  def setUnicodeStream (index : Int, x : InputStream, length : Int) {
      underlying.setUnicodeStream(index, x, length)
      paramMap += index -> "(Unicode Stream : %s (%d bytes))".format(x, length)
  }

  def setURL (index : Int, x : URL) {
      underlying.setURL(index, x)
      paramMap += index -> "\"%s\"".format(x)
  }
}

object StatementConstantDescriptions {
    def genKeyDescriptions (in : Int) = in match {
        case Statement.NO_GENERATED_KEYS => "NO_GENERATED_KEYS"
        case Statement.RETURN_GENERATED_KEYS => "RETURN_GENERATED_KEYS"
        case x => "Invalid Generated Keys Constant: " + x
    }

    def fetchDirDescriptions (in : Int) = in match {
        case ResultSet.FETCH_FORWARD => "FETCH_FORWARD"
        case ResultSet.FETCH_REVERSE => "FETCH_REVERSE"
        case ResultSet.FETCH_UNKNOWN => "FETCH_UNKNOWN"
        case x => "Invalid Fetch Direction Constant: " + x
    }

    def getMoreResultsDescriptions (in : Int) = in match {
        case Statement.CLOSE_CURRENT_RESULT => "CLOSE_CURRENT_RESULT"
        case Statement.KEEP_CURRENT_RESULT => "KEEP_CURRENT_RESULT"
        case Statement.CLOSE_ALL_RESULTS => "CLOSE_ALL_RESULTS"
        case x => "Invalid getMoreResults constant: " + x
    }

    def resultSetConcurrencyDescs (in : Int) = in match {
        case ResultSet.CONCUR_READ_ONLY => "CONCUR_READ_ONLY"
        case ResultSet.CONCUR_UPDATABLE => "CONCUR_UPDATABLE"
        case x => "Invalid ResultSet concurrency constant: " + x
    }

    def resultSetHoldabilityDescs (in : Int) = in match {
        case ResultSet.HOLD_CURSORS_OVER_COMMIT => "HOLD_CURSORS_OVER_COMMIT"
        case ResultSet.CLOSE_CURSORS_AT_COMMIT => "CLOSE_CURSORS_AT_COMMIT"
        case x => "Invalid ResultSet holdability constant: " + x
    }

    def resultSetTypeDescs (in : Int) = in match {
        case ResultSet.TYPE_FORWARD_ONLY => "TYPE_FORWARD_ONLY"
        case ResultSet.TYPE_SCROLL_INSENSITIVE => "TYPE_SCROLL_INSENSITIVE"
        case ResultSet.TYPE_SCROLL_SENSITIVE => "TYPE_SCROLL_SENSITIVE"
        case x => "Invalid ResultSet type constant: " + x
    }
}