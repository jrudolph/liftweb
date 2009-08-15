package net.liftweb.mapper

/*
 * Copyright 2006-2009 WorldWide Conferencing, LLC
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

import _root_.java.sql.{Connection, ResultSet, Statement, PreparedStatement, Types, ResultSetMetaData}
import _root_.javax.sql.{ DataSource}
import _root_.javax.naming.{Context, InitialContext}
import _root_.scala.collection.mutable._
import _root_.net.liftweb.util._
import _root_.net.liftweb.http._
import Helpers._

object DB {
  private val threadStore = new ThreadLocal[HashMap[ConnectionIdentifier, ConnectionHolder]]
  private val envContext = FatLazy((new InitialContext).lookup("java:/comp/env").asInstanceOf[Context])

  var globalDefaultSchemaName: Box[String] = Empty

  var queryTimeout: Box[Int] = Empty

  private var logFuncs: List[(String, Long) => Any] = Nil

  def addLogFunc(f: (String, Long) => Any): List[(String, Long) => Any] = {
    logFuncs = logFuncs ::: List(f)
    logFuncs
  }

  /**
   * can we get a JDBC connection from JNDI?
   */
  def jndiJdbcConnAvailable_? : Boolean = {
    val touchedEnv = envContext.calculated_?

    val ret = try {
      (envContext.get.lookup(DefaultConnectionIdentifier.jndiName).asInstanceOf[DataSource].getConnection) != null
    } catch {
      case e => false
    }

    if (!touchedEnv) envContext.reset
    ret
  }

  // var connectionManager: Box[ConnectionManager] = Empty
  private val connectionManagers = new HashMap[ConnectionIdentifier, ConnectionManager];

  def defineConnectionManager(name: ConnectionIdentifier, mgr: ConnectionManager) {
    connectionManagers(name) = mgr
  }

  case class ConnectionHolder(conn: SuperConnection, cnt: Int, postCommit: List[() => Unit])

  private def info : HashMap[ConnectionIdentifier, ConnectionHolder] = {
    threadStore.get match {
      case null =>
        val tinfo = new HashMap[ConnectionIdentifier, ConnectionHolder]
        threadStore.set(tinfo)
        tinfo

      case v => v
    }
  }

  // remove thread-local association
  def clearThread: Unit = {
    val i = info
    val ks = i.keySet
    if (ks.isEmpty)
    threadStore.remove
    else {
      ks.foreach(n => releaseConnectionNamed(n))
      clearThread
    }
  }

  private def newConnection(name : ConnectionIdentifier) : SuperConnection = {
    val ret = (Box(connectionManagers.get(name)).flatMap(cm => cm.newSuperConnection(name) or cm.newConnection(name).map(c => new SuperConnection(c, () => cm.releaseConnection(c))))) openOr {
      Helpers.tryo {
        val uniqueId = if (Log.isDebugEnabled) Helpers.nextNum.toString else ""
        Log.debug("Connection ID "+uniqueId+" for JNDI connection "+name.jndiName+" opened")
        val conn = envContext.get.lookup(name.jndiName).asInstanceOf[DataSource].getConnection
        new SuperConnection(conn, () => {Log.debug("Connection ID "+uniqueId+" for JNDI connection "+name.jndiName+" closed"); conn.close})
      } openOr {throw new NullPointerException("Looking for Connection Identifier "+name+" but failed to find either a JNDI data source "+
                                               "with the name "+name.jndiName+" or a lift connection manager with the correct name")}
    }
    ret.setAutoCommit(false)
    ret
  }

  /**
   * Build a LoanWrapper to pass into S.addAround() to make requests for
   * the DefaultConnectionIdentifier transactional for the complete HTTP request
   */
  def buildLoanWrapper(): LoanWrapper =
  buildLoanWrapper(List(DefaultConnectionIdentifier))

  /**
   * Build a LoanWrapper to pass into S.addAround() to make requests for
   * the List of ConnectionIdentifiers transactional for the complete HTTP request
   */
  def buildLoanWrapper(in: List[ConnectionIdentifier]): LoanWrapper =
  new LoanWrapper {
    private def doWith[T](in: List[ConnectionIdentifier], f: => T): T =
    in match {
      case Nil => f
      case x :: xs => use(x)(ignore => doWith(xs, f))
    }
    private object DepthCnt extends RequestVar(0)
    def apply[T](f: => T): T =
    try {
      DepthCnt.update(_ + 1)
      doWith(in, f)
    } finally {
      DepthCnt.update(_ - 1)
      if (DepthCnt.is == 0) clearThread
    }
  }

  private def releaseConnection(conn : SuperConnection): Unit = conn.close

  private def getConnection(name : ConnectionIdentifier): SuperConnection =  {
    Log.trace("Acquiring connection "+name+" On thread "+Thread.currentThread)
    var ret = info.get(name) match {
      case None => ConnectionHolder(newConnection(name), 1, Nil)
      case Some(ConnectionHolder(conn, cnt, post)) => ConnectionHolder(conn, cnt + 1, post)
    }
    info(name) = ret
    Log.trace("Acquired connection "+name+" on thread "+Thread.currentThread+
    " count "+ret.cnt)
    ret.conn
  }

  private def releaseConnectionNamed(name: ConnectionIdentifier) {
    Log.trace("Request to release connection: "+name+" on thread "+Thread.currentThread)
    (info.get(name): @unchecked) match {
      case Some(ConnectionHolder(c, 1, post)) =>
        c.commit
        tryo(c.releaseFunc())
        info -= name
        post.reverse.foreach(f => tryo(f()))
        Log.trace("Released connection "+name+" on thread "+Thread.currentThread)

      case Some(ConnectionHolder(c, n, post)) =>
        Log.trace("Did not release connection: "+name+" on thread "+Thread.currentThread+" count "+(n - 1))
        info(name) = ConnectionHolder(c, n - 1, post)

      case _ =>
        // ignore
    }
  }

  /**
   *  Append a function to be invoked after the commit has taken place for the given connection identifier
   */
  def appendPostFunc(name: ConnectionIdentifier, func: () => Unit) {
    info.get(name) match {
      case Some(ConnectionHolder(c, n, post)) => info(name) = ConnectionHolder(c, n, func :: post)
      case _ =>
    }
  }

  private def runLogger(query: String, time: Long) {
    logFuncs.foreach(_(query, time))
  }

  def statement[T](db : SuperConnection)(f : (Statement) => T) : T =  {
    Helpers.calcTime {
      val st =
        if (logFuncs.isEmpty) {
            db.createStatement
        } else {
            new LoggedStatement(db.createStatement)
        }
      queryTimeout.foreach(to => st.setQueryTimeout(to))
      try {
        (st.toString, f(st))
      } finally {
        st.close
      }
    } match {case (time, (query, res)) => runLogger(query, time); res}
  }

  def exec[T](db : SuperConnection, query : String)(f : (ResultSet) => T) : T = {
    Helpers.calcTime(
      statement(db) {st =>
        f(st.executeQuery(query))
      }) match {case (time, res) => runLogger(query, time); res} // TODO: This will double-log. Should it be modified?
  }



  private def asString(pos: Int, rs: ResultSet, md: ResultSetMetaData): String = {
    import _root_.java.sql.Types._
    md.getColumnType(pos) match {
      case ARRAY | BINARY | BLOB | DATALINK | DISTINCT | JAVA_OBJECT | LONGVARBINARY | NULL | OTHER | REF | STRUCT | VARBINARY  => rs.getObject(pos) match {
          case null => null
          case s => s.toString
        }
      case BIGINT |  INTEGER | DECIMAL | NUMERIC | SMALLINT | TINYINT => rs.getLong(pos).toString
      case BIT | BOOLEAN => rs.getBoolean(pos).toString

      case VARCHAR | CHAR | CLOB | LONGVARCHAR => rs.getString(pos)

      case DATE | TIME | TIMESTAMP => rs.getTimestamp(pos).toString

      case DOUBLE | FLOAT | REAL  => rs.getDouble(pos).toString
    }
  }

  def resultSetTo(rs: ResultSet): (List[String], List[List[String]]) = {
    val md = rs.getMetaData
    val cnt = md.getColumnCount
    val cntList = (1 to cnt).toList
    val colNames = cntList.map(i => md.getColumnName(i))

    val lb = new ListBuffer[List[String]]()

    while(rs.next) {
      lb += cntList.map(i => asString(i, rs, md))
    }

    (colNames, lb.toList)
  }

  /**
   * Executes the given parameterized query string with the given parameters.
   * Parameters are substituted in order. For Date/Time types, passing a java.util.Date will result in a
   * Timestamp parameter. If you want a specific SQL Date/Time type, use the corresponding
   * java.sql.Date, java.sql.Time, or java.sql.Timestamp classes.
   */
  def runQuery(query: String, params: List[Any]): (List[String], List[List[String]]) = {
    use(DefaultConnectionIdentifier)(conn => prepareStatement(query, conn) {
        ps =>
        params.zipWithIndex.foreach {
          case (null, idx) => ps.setNull(idx + 1, Types.VARCHAR)
          case (i: Int, idx) => ps.setInt(idx +1, i)
          case (l: Long, idx) => ps.setLong(idx + 1, l)
          case (d: Double, idx) => ps.setDouble(idx + 1, d)
          case (f: Float, idx) => ps.setFloat(idx + 1, f)
	  // Allow the user to specify how they want the Date handled based on the input type
	  case (t: _root_.java.sql.Timestamp, idx) => ps.setTimestamp(idx + 1, t)
	  case (d: _root_.java.sql.Date, idx) => ps.setDate(idx + 1, d)
	  case (t: _root_.java.sql.Time, idx) => ps.setTime(idx + 1, t)
	  /* java.util.Date has to go last, since the java.sql date/time classes subclass it. By default we
	   * assume a Timestamp value */
          case (d: _root_.java.util.Date, idx) => ps.setTimestamp(idx + 1, new _root_.java.sql.Timestamp(d.getTime))
          case (b: Boolean, idx) => ps.setBoolean(idx + 1, b)
          case (s: String, idx) => ps.setString(idx + 1, s)
          case (bn: _root_.java.math.BigDecimal, idx) => ps.setBigDecimal(idx + 1, bn)
          case (obj, idx) => ps.setObject(idx + 1, obj)
        }

        resultSetTo(ps.executeQuery)
      })
  }

  def runQuery(query: String): (List[String], List[List[String]]) = {


    use(DefaultConnectionIdentifier)(conn => exec(conn, query)(resultSetTo))
  }

  def rollback(name: ConnectionIdentifier) = use(name)(conn => conn.rollback)

  /**
   * Executes {@code statement} and converts the {@code ResultSet} to model
   * instance {@code T} using {@code f}
   */
  def exec[T](statement : PreparedStatement)(f : (ResultSet) => T) : T = {
    queryTimeout.foreach(to => statement.setQueryTimeout(to))
    Helpers.calcTime {
      val rs = statement.executeQuery
      try {
        (statement.toString, f(rs))
      } finally {
        statement.close
        rs.close
      }} match {case (time, (query, res)) => runLogger(query, time); res}
  }

  def prepareStatement[T](statement : String, conn: SuperConnection)(f : (PreparedStatement) => T) : T = {
    Helpers.calcTime {
      val st =
        if (logFuncs.isEmpty) {
            conn.prepareStatement(statement)
        } else {
            new LoggedPreparedStatement (statement, conn.prepareStatement(statement))
        }
      queryTimeout.foreach(to => st.setQueryTimeout(to))
      try {
        (st.toString, f(st))
      } finally {
        st.close
      }} match {case (time, (query, res)) => runLogger(query, time); res}
  }

  def prepareStatement[T](statement : String, keys: Int, conn: SuperConnection)(f : (PreparedStatement) => T) : T = {
    Helpers.calcTime{
      val st =
        if (logFuncs.isEmpty) {
            conn.prepareStatement(statement, keys)
        } else {
            new LoggedPreparedStatement(statement, conn.prepareStatement(statement, keys))
        }
      queryTimeout.foreach(to => st.setQueryTimeout(to))
      try {
        (st.toString, f(st))
      } finally {
        st.close
      }} match {case (time, (query, res)) => runLogger(query, time); res}
  }

  /**
   * Executes function {@code f} with the connection named {@code name}. Releases the connection
   * before returning.
   */
  def use[T](name : ConnectionIdentifier)(f : (SuperConnection) => T) : T = {
    val conn = getConnection(name)
    try {
      f(conn)
    } finally {
        releaseConnectionNamed(name)
    }
  }



  val reservedWords = _root_.scala.collection.immutable.HashSet.empty ++
  List("abort" ,
       "accept" ,
       "access" ,
       "add" ,
       "admin" ,
       "after" ,
       "all" ,
       "allocate" ,
       "alter" ,
       "analyze" ,
       "and" ,
       "any" ,
       "archive" ,
       "archivelog" ,
       "array" ,
       "arraylen" ,
       "as" ,
       "asc" ,
       "assert" ,
       "assign" ,
       "at" ,
       "audit" ,
       "authorization" ,
       "avg" ,
       "backup" ,
       "base_table" ,
       "become" ,
       "before" ,
       "begin" ,
       "between" ,
       "binary_integer" ,
       "block" ,
       "body" ,
       "boolean" ,
       "by" ,
       "cache" ,
       "cancel" ,
       "cascade" ,
       "case" ,
       "change" ,
       "char" ,
       "character" ,
       "char_base" ,
       "check" ,
       "checkpoint" ,
       "close" ,
       "cluster" ,
       "clusters" ,
       "cobol" ,
       "colauth" ,
       "column" ,
       "columns" ,
       "comment" ,
       "commit" ,
       "compile" ,
       "compress" ,
       "connect" ,
       "constant" ,
       "constraint" ,
       "constraints" ,
       "contents" ,
       "continue" ,
       "controlfile" ,
       "count" ,
       "crash" ,
       "create" ,
       "current" ,
       "currval" ,
       "cursor" ,
       "cycle" ,
       "database" ,
       "data_base" ,
       "datafile" ,
       "date" ,
       "dba" ,
       "debugoff" ,
       "debugon" ,
       "dec" ,
       "decimal" ,
       "declare" ,
       "default" ,
       "definition" ,
       "delay" ,
       "delete" ,
       "delta" ,
       "desc" ,
       "digits" ,
       "disable" ,
       "dismount" ,
       "dispose" ,
       "distinct" ,
       "do" ,
       "double" ,
       "drop" ,
       "dump" ,
       "each" ,
       "else" ,
       "elsif" ,
       "enable" ,
       "end" ,
       "entry" ,
       "escape" ,
       "events" ,
       "except" ,
       "exception" ,
       "exception_init" ,
       "exceptions" ,
       "exclusive" ,
       "exec" ,
       "execute" ,
       "exists" ,
       "exit" ,
       "explain" ,
       "extent" ,
       "externally" ,
       "false" ,
       "fetch" ,
       "file" ,
       "float" ,
       "flush" ,
       "for" ,
       "force" ,
       "foreign" ,
       "form" ,
       "fortran" ,
       "found" ,
       "freelist" ,
       "freelists" ,
       "from" ,
       "function" ,
       "generic" ,
       "go" ,
       "goto" ,
       "grant" ,
       "group" ,
       "having" ,
       "identified" ,
       "if" ,
       "immediate" ,
       "in" ,
       "including" ,
       "increment" ,
       "index" ,
       "indexes" ,
       "indicator" ,
       "initial" ,
       "initrans" ,
       "insert" ,
       "instance" ,
       "int" ,
       "integer" ,
       "intersect" ,
       "into" ,
       "is" ,
       "key" ,
       "language" ,
       "layer" ,
       "level" ,
       "like" ,
       "limited" ,
       "link" ,
       "lists" ,
       "lock" ,
       "logfile" ,
       "long" ,
       "loop" ,
       "manage" ,
       "manual" ,
       "max" ,
       "maxdatafiles" ,
       "maxextents" ,
       "maxinstances" ,
       "maxlogfiles" ,
       "maxloghistory" ,
       "maxlogmembers" ,
       "maxtrans" ,
       "maxvalue" ,
       "min" ,
       "minextents" ,
       "minus" ,
       "minvalue" ,
       "mlslabel" ,
       "mod" ,
       "mode" ,
       "modify" ,
       "module" ,
       "mount" ,
       "natural" ,
       "new" ,
       "next" ,
       "nextval" ,
       "noarchivelog" ,
       "noaudit" ,
       "nocache" ,
       "nocompress" ,
       "nocycle" ,
       "nomaxvalue" ,
       "nominvalue" ,
       "none" ,
       "noorder" ,
       "noresetlogs" ,
       "normal" ,
       "nosort" ,
       "not" ,
       "notfound" ,
       "nowait" ,
       "null" ,
       "number" ,
       "number_base" ,
       "numeric" ,
       "of" ,
       "off" ,
       "offline" ,
       "old" ,
       "on" ,
       "online" ,
       "only" ,
       "open" ,
       "optimal" ,
       "option" ,
       "or" ,
       "order" ,
       "others" ,
       "out" ,
       "own" ,
       "package" ,
       "parallel" ,
       "partition" ,
       "pctfree" ,
       "pctincrease" ,
       "pctused" ,
       "plan" ,
       "pli" ,
       "positive" ,
       "pragma" ,
       "precision" ,
       "primary" ,
       "prior" ,
       "private" ,
       "privileges" ,
       "procedure" ,
       "profile" ,
       "public" ,
       "quota" ,
       "raise" ,
       "range" ,
       "raw" ,
       "read" ,
       "real" ,
       "record" ,
       "recover" ,
       "references" ,
       "referencing" ,
       "release" ,
       "remr" ,
       "rename" ,
       "resetlogs" ,
       "resource" ,
       "restricted" ,
       "return" ,
       "reuse" ,
       "reverse" ,
       "revoke" ,
       "role" ,
       "roles" ,
       "rollback" ,
       "row" ,
       "rowid" ,
       "rowlabel" ,
       "rownum" ,
       "rows" ,
       "rowtype" ,
       "run" ,
       "savepoint" ,
       "schema" ,
       "scn" ,
       "section" ,
       "segment" ,
       "select" ,
       "separate" ,
       "sequence" ,
       "session" ,
       "set" ,
       "share" ,
       "shared" ,
       "size" ,
       "smallint" ,
       "snapshot" ,
       "some" ,
       "sort" ,
       "space" ,
       "sql" ,
       "sqlbuf" ,
       "sqlcode" ,
       "sqlerrm" ,
       "sqlerror" ,
       "sqlstate" ,
       "start" ,
       "statement" ,
       "statement_id" ,
       "statistics" ,
       "stddev" ,
       "stop" ,
       "storage" ,
       "subtype" ,
       "successful" ,
       "sum" ,
       "switch" ,
       "synonym" ,
       "sysdate" ,
       "system" ,
       "tabauth" ,
       "table" ,
       "tables" ,
       "tablespace" ,
       "task" ,
       "temporary" ,
       "terminate" ,
       "then" ,
       "thread" ,
       "time" ,
       "to" ,
       "tracing" ,
       "transaction" ,
       "trigger" ,
       "triggers" ,
       "true" ,
       "truncate" ,
       "type" ,
       "uid" ,
       "under" ,
       "union" ,
       "unique" ,
       "unlimited" ,
       "until" ,
       "update" ,
       "use" ,
       "user" ,
       "using" ,
       "validate" ,
       "values" ,
       "varchar" ,
       "varchar2" ,
       "variance" ,
       "view" ,
       "views" ,
       "when" ,
       "whenever" ,
       "where" ,
       "while" ,
       "with" ,
       "work" ,
       "write" ,
       "xor")
}

class SuperConnection(val connection: Connection,val releaseFunc: () => Unit, val schemaName: Box[String]) {
  def this(c: Connection, rf: () => Unit) = this(c, rf, Empty)
  
  lazy val brokenLimit_? = driverType.brokenLimit_?
  lazy val brokenAutogeneratedKeys_? = driverType.brokenAutogeneratedKeys_?
  lazy val wickedBrokenAutogeneratedKeys_? = driverType.wickedBrokenAutogeneratedKeys_?


  def createTablePostpend: String = driverType.createTablePostpend
  def supportsForeignKeys_? : Boolean = driverType.supportsForeignKeys_?

  lazy val driverType = (calcDriver(connection.getMetaData.getDatabaseProductName))

  def calcDriver(name: String): DriverType = {
    name match {
      case DerbyDriver.name => DerbyDriver
      case MySqlDriver.name => MySqlDriver
      case PostgreSqlDriver.name => PostgreSqlDriver
      case H2Driver.name => H2Driver
      case SqlServerDriver.name => SqlServerDriver
      case OracleDriver.name => OracleDriver
      case MaxDbDriver.name => MaxDbDriver
    }
  }
}

object SuperConnection {
  implicit def superToConn(in: SuperConnection): Connection = in.connection
}

trait ConnectionIdentifier {
  def jndiName: String
  override def toString() = "ConnectionIdentifier("+jndiName+")"
  override def hashCode() = jndiName.hashCode()
  override def equals(other: Any): Boolean = other match {
    case ci: ConnectionIdentifier => ci.jndiName == this.jndiName
    case _ => false
  }
}

case object DefaultConnectionIdentifier extends ConnectionIdentifier {
  var jndiName = "lift"
}
