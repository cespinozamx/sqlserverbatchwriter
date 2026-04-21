// Databricks notebook source
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, spark_partition_id}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import java.sql.{CallableStatement, Connection, DriverManager, PreparedStatement, ResultSet, Types}
import java.util.Properties
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, Future, Semaphore}
import scala.util.Try
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

// COMMAND ----------

// ── BulkColMeta y SparkRowBulkRecord ─────────────────────────────────────────
// Top-level: accesibles desde BulkWorker (workers Spark) y SqlServerBatchWriter.
case class BulkColMeta(name: String, jdbcType: Int, precision: Int, scale: Int)

class SparkRowBulkRecord(
  rows    : Array[org.apache.spark.sql.Row],
  schema  : org.apache.spark.sql.types.StructType,
  columns : List[String]
) extends com.microsoft.sqlserver.jdbc.ISQLServerBulkData {

  import java.util.{Set => JSet, LinkedHashSet => JLinkedHashSet}
  import java.sql.Types
  import org.apache.spark.sql.types._
  import scala.util.Try

  private var cursor = -1
  private val ncols  = columns.size

  private val colMeta: Array[BulkColMeta] = columns.zipWithIndex.map { case (c, _) =>
    val t = Try(schema(c).dataType).getOrElse(StringType)
    val (jt, pr, sc) = t match {
      case IntegerType     => (Types.INTEGER,   10, 0)
      case LongType        => (Types.BIGINT,    19, 0)
      case DoubleType      => (Types.DOUBLE,    15, 0)
      case FloatType       => (Types.REAL,       7, 0)
      case dt: DecimalType => (Types.DECIMAL,   dt.precision, dt.scale)
      case DateType        => (Types.DATE,       10, 0)
      case TimestampType   => (Types.TIMESTAMP,  23, 3)
      case BooleanType     => (Types.BIT,         1, 0)
      case StringType      => (Types.NVARCHAR,  4000, 0)
      case BinaryType      => (Types.VARBINARY, 8000, 0)
      case ByteType        => (Types.TINYINT,    3, 0)
      case ShortType       => (Types.SMALLINT,   5, 0)
      case _               => (Types.NVARCHAR,  4000, 0)
    }
    BulkColMeta(c, jt, pr, sc)
  }.toArray

  private type Ext = org.apache.spark.sql.Row => Object
  private val extractors: Array[Ext] = columns.zipWithIndex.map { case (c, i) =>
    val t = Try(schema(c).dataType).getOrElse(StringType)
    val fn: Ext = t match {
      case IntegerType     => r => (r.getInt(i):     java.lang.Integer)
      case LongType        => r => (r.getLong(i):    java.lang.Long)
      case DoubleType      => r => (r.getDouble(i):  java.lang.Double)
      case FloatType       => r => (r.getFloat(i):   java.lang.Float)
      case _: DecimalType  => r => r.getDecimal(i)
      case DateType        => r => r.getAs[java.sql.Date](i)
      case TimestampType   => r => r.getTimestamp(i)
      case BooleanType     => r => (r.getBoolean(i): java.lang.Boolean)
      case BinaryType      => r => r.getAs[Array[Byte]](i).asInstanceOf[Object]
      case ByteType        => r => (r.getByte(i).toInt: java.lang.Integer)
      case ShortType       => r => (r.getShort(i).toInt: java.lang.Integer)
      case _               => r => r.getString(i)
    }
    fn
  }.toArray

  private val ordinals: JSet[Integer] = {
    val s = new JLinkedHashSet[Integer]()
    (1 to ncols).foreach(i => s.add(i))
    s
  }

  override def next(): Boolean = { cursor += 1; cursor < rows.length }

  override def getRowData(): Array[Object] = {
    val row  = rows(cursor)
    val data = new Array[Object](ncols)
    var i = 0
    while (i < ncols) {
      data(i) = if (row.isNullAt(i)) null else extractors(i)(row)
      i += 1
    }
    data
  }

  override def getColumnOrdinals()     : JSet[Integer] = ordinals
  override def getColumnName(col: Int) : String  = if (col >= 1 && col <= ncols) colMeta(col-1).name      else ""
  override def getColumnType(col: Int) : Int     = if (col >= 1 && col <= ncols) colMeta(col-1).jdbcType  else Types.NVARCHAR
  override def getPrecision(col: Int)  : Int     = if (col >= 1 && col <= ncols) colMeta(col-1).precision else 0
  override def getScale(col: Int)      : Int     = if (col >= 1 && col <= ncols) colMeta(col-1).scale     else 0
}


// ── BulkWorker ────────────────────────────────────────────────────────────────
// Object top-level sin campos: garantiza serialización completa en Spark Connect.
// executeBlocks: punto de entrada para foreachPartition en Job Cluster Dedicated.
// loadPartition : lógica de lectura Delta + BulkCopy ejecutada en el worker JVM.

object BulkWorker {

  def executeDirect(
    df           : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    jdbcUrl      : String,
    targetTable  : String,
    filteredCols : List[String],
    schemaJson   : String,
    tableLock    : Boolean,
    timeoutSecs  : Int
  ): Unit = {
    val ju_p = jdbcUrl; val tt_p = targetTable; val fc_p = filteredCols
    val sj_p = schemaJson; val tl_p = tableLock; val to_p = timeoutSecs

    df.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      val partRows = rows.toArray
      if (partRows.nonEmpty) {
        val schema = org.apache.spark.sql.types.DataType
          .fromJson(sj_p).asInstanceOf[org.apache.spark.sql.types.StructType]
        val opts = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions()
        opts.setBatchSize(0); opts.setCheckConstraints(false)
        opts.setTableLock(tl_p); opts.setBulkCopyTimeout(to_p)
        val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(ju_p)
        try {
          bc.setBulkCopyOptions(opts); bc.setDestinationTableName(tt_p)
          fc_p.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
          bc.writeToServer(new SparkRowBulkRecord(partRows, schema, fc_p))
        } finally { scala.util.Try(bc.close()) }
      }
    }
  }

  def executeBlocks(
    blocksDf      : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    deltaSource   : String,
    isDeltaTable  : Boolean,
    partitionCol  : String,
    targetTable   : String,
    jdbcUrl       : String,
    filteredCols  : List[String],
    schemaJson    : String,
    tableLock     : Boolean,
    bulkCopyBatch : Int,
    bulkTimeout   : Int,
    blockRowLimit : Int,
    accOk         : org.apache.spark.util.LongAccumulator,
    accFail       : org.apache.spark.util.LongAccumulator,
    accRows       : org.apache.spark.util.LongAccumulator
  ): Unit = {
    val ds_p = deltaSource; val dt_p = isDeltaTable; val pc_p = partitionCol
    val tt_p = targetTable; val ju_p = jdbcUrl;      val fc_p = filteredCols
    val sj_p = schemaJson;  val tl_p = tableLock;    val bcb_p = bulkCopyBatch
    val bt_p = bulkTimeout; val brl_p = blockRowLimit
    val aok_p = accOk; val afail_p = accFail; val arows_p = accRows

    blocksDf.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      rows.foreach { row =>
        val blockId = row.getInt(0)
        val t0      = System.currentTimeMillis()
        val (bid, rowCount, errMsg) = BulkWorker.loadPartition(
          blockId, ds_p, dt_p, pc_p, tt_p, ju_p, fc_p, sj_p, tl_p, bcb_p, bt_p, brl_p
        )
        val elapsed = (System.currentTimeMillis() - t0) / 1000
        if (errMsg.isEmpty) {
          aok_p.add(1L); arows_p.add(rowCount)
          println(s"[WORKER] \u2713 block=$bid rows=$rowCount ${elapsed}s")
        } else {
          afail_p.add(1L)
          println(s"[WORKER] \u2717 block=$bid ${elapsed}s: ${errMsg.getOrElse("error")}")
        }
      }
    }
  }

  def loadPartition(
    partId        : Int,
    deltaSource   : String,
    isDeltaTable  : Boolean,
    partitionCol  : String,
    targetTable   : String,
    jdbcUrl       : String,
    filteredCols  : List[String],
    schemaJson    : String,
    tableLock     : Boolean,
    bulkCopyBatch : Int,
    bulkTimeout   : Int,
    blockRowLimit : Int = 0
  ): (Int, Long, Option[String]) = {
    try {
      val ws = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(
        throw new IllegalStateException("SparkSession no disponible en worker"))
      val schema = org.apache.spark.sql.types.DataType
        .fromJson(schemaJson).asInstanceOf[org.apache.spark.sql.types.StructType]
      val blockDf = {
        val base = if (isDeltaTable) ws.table(deltaSource)
                   else ws.read.format("delta").load(deltaSource)
        base.filter(s"$partitionCol = $partId").drop(partitionCol)
      }
      val opts = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions()
      opts.setBatchSize(bulkCopyBatch)
      opts.setCheckConstraints(false)
      opts.setTableLock(tableLock)
      opts.setBulkCopyTimeout(bulkTimeout)
      var totalInserted = 0L

      if (blockRowLimit <= 0) {
        val rows = blockDf.collect()
        if (rows.isEmpty) return (partId, 0L, None)
        val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrl)
        try {
          bc.setBulkCopyOptions(opts); bc.setDestinationTableName(targetTable)
          filteredCols.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
          bc.writeToServer(new SparkRowBulkRecord(rows, schema, filteredCols))
          totalInserted = rows.length.toLong
        } finally { scala.util.Try(bc.close()) }
      } else {
        val allRows = blockDf.collect()
        if (allRows.isEmpty) return (partId, 0L, None)
        allRows.grouped(blockRowLimit).foreach { chunk =>
          val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrl)
          try {
            bc.setBulkCopyOptions(opts); bc.setDestinationTableName(targetTable)
            filteredCols.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
            bc.writeToServer(new SparkRowBulkRecord(chunk, schema, filteredCols))
            totalInserted += chunk.length.toLong
          } catch {
            case ex: Throwable =>
              scala.util.Try(bc.close())
              return (partId, totalInserted, Some(s"chunk@$totalInserted: ${ex.getMessage}"))
          } finally { scala.util.Try(bc.close()) }
        }
      }
      (partId, totalInserted, None)
    } catch {
      case ex: Throwable => (partId, 0L, Some(ex.getMessage))
    }
  }
}

object SqlServerBatchWriter {

  import org.apache.spark.sql.SparkSession

  // ── Tipos auxiliares ──────────────────────────────────────────────────────

  case class SpParam(name: String, value: Option[Any] = None, sqlType: Option[Int] = None, isOutput: Boolean = false)
  object SpParam {
    def in   (name: String, value: Any)               = SpParam(name, Some(value))
    def out  (name: String, sqlType: Int)             = SpParam(name, None, Some(sqlType), isOutput = true)
    def inOut(name: String, value: Any, sqlType: Int) = SpParam(name, Some(value), Some(sqlType), isOutput = true)
  }

  case class BatchResult(
    batchId  : String,
    partId   : Int,
    rowCount : Long,
    success  : Boolean,
    errorMsg : String = ""
  )

  // RunResult: resultado estructurado devuelto por todos los puntos de entrada.
  case class RunResult(
    success      : Boolean,
    totalParts   : Int,
    successParts : Int,
    failedParts  : Int,
    pendingParts : Int,
    errorMessage : Option[String] = None,
    runDate      : java.sql.Date  = new java.sql.Date(System.currentTimeMillis()),
    processName  : String         = "",
    country      : String         = ""
  ) {
    def isComplete : Boolean = failedParts == 0 && pendingParts == 0
    def needsRetry : Boolean = failedParts > 0  || pendingParts > 0
    def printSummary(): Unit = {
      val sep    = "=" * 66
      val estado = if (success) "COMPLETADO OK" else if (needsRetry) "PARCIAL — REINTENTAR" else "ERROR FATAL"
      println(s"\n$sep")
      println(s"  RESULTADO : $estado")
      println(s"  Proceso   : $processName | País: $country | Fecha: $runDate")
      println(s"  Partes    : total=$totalParts | OK=$successParts | FAILED=$failedParts | PENDING=$pendingParts")
      errorMessage.foreach(e => println(s"  Error     : $e"))
      println(s"$sep\n")
    }
  }

  // ── AppConfig ─────────────────────────────────────────────────────────────
  // Configuración unificada para los tres flujos de carga.
  // Todos los campos tienen defaults seguros — sobrescribir solo lo necesario.

  // AppConfig — configuración unificada para los tres flujos de carga.
  case class AppConfig(
    // Conexión JDBC
    jdbcUrl      : String = "",
    jdbcUser     : String = "",
    jdbcPassword : String = "",
    targetTable  : String = "",
    excludeColumns : List[String] = List("Id"),

    // Flujo A: JDBC directo (< 50M filas)
    // directBulkBatchSize: filas por chunk via toLocalIterator. Controla heap máximo en driver (~30 MB).
    directBulkBatchSize    : Int  = 50_000,
    directBulkTimeoutSecs  : Int  = 300,
    directBulkMaxRetries   : Int  = 3,
    directBulkRetryDelayMs : Long = 3_000L,

    // Flujo B: Parquet/Delta Landing (>= 50M filas)
    autoTuning          : Boolean         = false,
    forceParquetLanding : Option[Boolean] = None,
    parquetPath         : String = "",
    parquetPartitionCol : String = "_part_id",
    ntileColumn         : String  = "",
    ntileOrderCol       : String  = "",
    ntileOnDistinct     : Boolean = true,
    ntileEnableZorder   : Boolean = false,
    batchSize           : Long    = 500_000L,
    bulkCopyBatchSize   : Int     = 0,
    thresholdXLarge     : Long    = 50_000_000L,
    maxConcurrentPartitions : Int = 6,
    gcBatchSize         : Int     = 50,
    tableLockOnBulk     : Boolean = false,
    initialLoadMode     : Boolean = false,
    useParallelLoad     : Boolean = false,

    // Flujo C: Delta Blocks (tabla pre-particionada con columna BLOCK)
    isDeltaTable  : Boolean = true,
    blockRowLimit : Int     = 0,

    // Checkpoint Unity Catalog
    checkpointTable : String  = "",
    checkpointMode  : String  = "batch",
    processName     : String  = "",
    country         : String  = "",
    forceNewRun     : Boolean = false,
    maxAutoRetries  : Int     = 3,

    // Timeouts
    maxPartitionTimeoutSecs : Int = 1_800,

    // Stored Procedures
    spSocketTimeoutSecs : Int  = 0,
    spMaxRetries        : Int  = 3,
    spRetryDelayMs      : Long = 2_000L
  ) {
    def jdbcProps: Properties = {
      val p = new Properties()
      p.setProperty("user", jdbcUser); p.setProperty("password", jdbcPassword)
      p.setProperty("loginTimeout", "30")
      p.setProperty("sendStringParametersAsUnicode", "false")
      p.setProperty("responseBuffering", "adaptive")
      p
    }

    def jdbcUrlForBulkCopy: String = {
      val base = if (jdbcUrl.endsWith(";")) jdbcUrl else jdbcUrl + ";"
      base +
        s"user=$jdbcUser;password=$jdbcPassword;" +
        s"loginTimeout=30;sendStringParametersAsUnicode=false;responseBuffering=adaptive;" +
        s"socketTimeout=${maxPartitionTimeoutSecs * 1000};" +
        s"connectRetryCount=3;connectRetryInterval=10;" +
        s"allowEncryptedValueModifications=true;multiSubnetFailover=false;"
    }

    def requiresCheckpoint: Boolean = checkpointTable.nonEmpty
    def validateCheckpointConfig(): Unit = {
      if (requiresCheckpoint) {
        require(processName.nonEmpty, "[CONFIG] processName requerido con checkpointTable.")
        require(country.nonEmpty,     "[CONFIG] country requerido con checkpointTable.")
      }
    }
  }

  private def writeDirectBulk(df: DataFrame, totalRows: Long, cfg: AppConfig)
                              (implicit spark: SparkSession): Unit = {
    val numBlocks = math.max(1, math.ceil(totalRows.toDouble / cfg.directBulkBatchSize).toInt)
    println(s"[DIRECT] ${cfg.targetTable} | filas=$totalRows | bloques=$numBlocks | batch=${cfg.directBulkBatchSize}")

    val excludeSet   = cfg.excludeColumns.map(_.toLowerCase).toSet
    val filteredCols = df.columns.filterNot(c => excludeSet.contains(c.toLowerCase)).toList
    val filteredDf   = df.select(filteredCols.map(col): _*).repartition(numBlocks)
    val schema       = filteredDf.schema
    val jdbcUrlBulk  = cfg.jdbcUrlForBulkCopy
    val targetTable  = cfg.targetTable
    val tableLock    = cfg.tableLockOnBulk
    val timeoutSecs  = cfg.directBulkTimeoutSecs
    val batchSize    = cfg.directBulkBatchSize

    val errors   = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    var inserted = 0L
    val t0       = System.currentTimeMillis()
    var chunkBuf = new scala.collection.mutable.ArrayBuffer[Row](batchSize)

    def flushChunk(chunk: Array[Row]): Unit = {
      val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrlBulk)
      try {
        val opts = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions()
        opts.setBatchSize(0); opts.setCheckConstraints(false)
        opts.setTableLock(tableLock); opts.setBulkCopyTimeout(timeoutSecs)
        bc.setBulkCopyOptions(opts); bc.setDestinationTableName(targetTable)
        filteredCols.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
        bc.writeToServer(new SparkRowBulkRecord(chunk, schema, filteredCols))
        inserted += chunk.length
      } catch {
        case ex: Throwable => errors.add(s"chunk@$inserted: ${ex.getMessage}")
      } finally { scala.util.Try(bc.close()) }
    }

    val iter = filteredDf.toLocalIterator()
    while (iter.hasNext && errors.isEmpty) {
      chunkBuf += iter.next()
      if (chunkBuf.size >= batchSize) {
        flushChunk(chunkBuf.toArray)
        chunkBuf = new scala.collection.mutable.ArrayBuffer[Row](batchSize)
      }
    }
    if (chunkBuf.nonEmpty && errors.isEmpty) flushChunk(chunkBuf.toArray)

    chunkBuf.clear()
    System.gc()

    val ms = System.currentTimeMillis() - t0
    if (errors.isEmpty) {
      println(s"[DIRECT] Completado: $inserted filas → $targetTable en ${ms}ms")
    } else {
      throw new RuntimeException(s"[DIRECT] BulkCopy falló: ${errors.toArray.mkString("; ")}")
    }
  }
  // ── Flujo B.1: writeToParquet — Fase 1: DataFrame → Delta en ADLS ────────

  def writeToParquet(df: DataFrame, totalRows: Long, cfg: AppConfig)
                    (implicit spark: SparkSession): Unit = {
    require(cfg.parquetPath.nonEmpty, "[F1] parquetPath requerido")
    val numParts = math.max(1, math.ceil(totalRows.toDouble / cfg.batchSize).toInt)
    val orderCol = if (cfg.ntileOrderCol.nonEmpty) cfg.ntileOrderCol else cfg.ntileColumn

    println(s"[F1] Path: ${cfg.parquetPath} | particiones=$numParts | ntile=${cfg.ntileColumn.nonEmpty}")

    val excludeSet   = cfg.excludeColumns.map(_.toLowerCase).toSet
    val filteredCols = df.columns.filterNot(c => excludeSet.contains(c.toLowerCase)).toList
    val filteredDf   = df.select(filteredCols.map(col): _*)

    val partitioned: DataFrame = if (cfg.ntileColumn.nonEmpty) {
      if (cfg.ntileOnDistinct) {
        import org.apache.spark.sql.expressions.Window
        val distinctDf  = filteredDf.select(col(cfg.ntileColumn)).distinct()
        val windowSpec  = Window.orderBy(col(orderCol))
        val tiledDistinct = distinctDf.withColumn(
          cfg.parquetPartitionCol,
          org.apache.spark.sql.functions.ntile(numParts).over(windowSpec).cast("int"))
        filteredDf.join(tiledDistinct, cfg.ntileColumn)
      } else {
        import org.apache.spark.sql.expressions.Window
        filteredDf.withColumn(
          cfg.parquetPartitionCol,
          org.apache.spark.sql.functions.ntile(numParts).over(Window.orderBy(col(orderCol))).cast("int"))
      }
    } else {
      filteredDf.repartition(numParts)
        .withColumn(cfg.parquetPartitionCol, spark_partition_id().cast("int"))
    }

    val pathStr = cfg.parquetPath.stripSuffix("/")
    Try { spark.read.format("delta").load(pathStr).limit(0).count() }.recover {
      case ex if ex.getMessage != null && (
        ex.getMessage.contains("DELTA_MISSING_TRANSACTION_LOG") ||
        ex.getMessage.contains("is not a Delta table")) =>
        Try { org.apache.hadoop.fs.FileSystem
          .get(new java.net.URI(pathStr), spark.sparkContext.hadoopConfiguration)
          .delete(new org.apache.hadoop.fs.Path(pathStr), true) }
    }

    val t0 = System.currentTimeMillis()
    partitioned.write.format("delta").mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy(cfg.parquetPartitionCol)
      .save(pathStr)
    println(s"[F1] Completado en ${(System.currentTimeMillis()-t0)/1000}s → $pathStr")
  }

  // ── Flujo B.2: loadFromParquet — Fase 2 driver-side (Standard cluster) ───
  // El driver lee Delta por partición y un pool de hilos JDBC hace BulkCopy.
  // Compatible con Standard cluster (Spark Connect).
  // NO usar cuando el bloque supera la capacidad del heap del driver.

  def loadFromParquet(numParts: Int, filteredCols: List[String],
                      runDate: java.sql.Date, cfg: AppConfig)
                     (implicit spark: SparkSession): Unit = {
    require(cfg.parquetPath.nonEmpty, "[F2] parquetPath requerido")
    val rt = Runtime.getRuntime
    def heapMB() = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024

    val useNtile = cfg.ntileColumn.nonEmpty
    val allParts = if (useNtile) (1 to numParts).toList else (0 until numParts).toList
    val completed = checkpointGetCompleted(runDate, cfg)
    val pending   = allParts.filterNot(completed.contains)

    println(s"[F2] total=$numParts | SUCCESS=${completed.size} | pendientes=${pending.size} | heap=${heapMB()}MB")
    if (pending.isEmpty) { println("[F2] Todas en SUCCESS."); return }

    val sampleId = allParts.head
    val partSchema: StructType = Try {
      val s = spark.read.format("delta").load(cfg.parquetPath)
        .filter(col(cfg.parquetPartitionCol) === sampleId)
        .drop(cfg.parquetPartitionCol).limit(1).schema
      println(s"[F2] Schema: ${s.fieldNames.mkString(", ")}")
      StructType(filteredCols.flatMap(c => s.fields.find(_.name.equalsIgnoreCase(c))))
    }.getOrElse(StructType(filteredCols.map(c => StructField(c, StringType))))

    val effectiveConc = if (cfg.initialLoadMode) 1 else cfg.maxConcurrentPartitions
    val semaphore     = new Semaphore(effectiveConc)
    val pool          = Executors.newFixedThreadPool(effectiveConc)
    val batchResults  = new ConcurrentHashMap[Int, BatchResult]()
    val workQueue     = new java.util.concurrent.ArrayBlockingQueue[(Int, Array[Row])](effectiveConc + 1)
    val t0            = System.currentTimeMillis()

    val tableLockEff = cfg.initialLoadMode || cfg.tableLockOnBulk
    val bulkBatchEff = if (cfg.initialLoadMode) 0 else cfg.bulkCopyBatchSize
    val jdbcUrlBulk  = cfg.jdbcUrlForBulkCopy

    def workerTask(): Runnable = new Runnable {
      def run(): Unit = {
        var active = true
        while (active) {
          val (partId, rows) = workQueue.take()
          if (partId == -1) { active = false }
          else {
            val bid  = java.util.UUID.randomUUID().toString.take(12)
            val tB   = System.currentTimeMillis()
            try {
              if (rows.nonEmpty) {
                val opts = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions()
                opts.setBatchSize(bulkBatchEff); opts.setCheckConstraints(false)
                opts.setTableLock(tableLockEff); opts.setBulkCopyTimeout(cfg.maxPartitionTimeoutSecs)
                val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrlBulk)
                try {
                  bc.setBulkCopyOptions(opts); bc.setDestinationTableName(cfg.targetTable)
                  filteredCols.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
                  bc.writeToServer(new SparkRowBulkRecord(rows, partSchema, filteredCols))
                } finally { scala.util.Try(bc.close()) }
                println(s"[F2] ✓ part=$partId rows=${rows.length} BULK=${System.currentTimeMillis()-tB}ms")
                batchResults.put(partId, BatchResult(bid, partId, rows.length.toLong, success = true))
              }
            } catch {
              case ex: Throwable =>
                println(s"[F2] ✗ part=$partId: ${ex.getMessage}")
                batchResults.put(partId, BatchResult(bid, partId, 0L, success = false, errorMsg = ex.getMessage))
            } finally { semaphore.release() }
          }
        }
      }
    }

    (0 until effectiveConc).foreach(_ => pool.submit(workerTask()))

    val lotes      = pending.grouped(math.max(1, cfg.gcBatchSize)).toList
    val totalLotes = lotes.size
    var loteIdx    = 0
    while (loteIdx < totalLotes) {
      val lote      = lotes(loteIdx)
      val heapAntes = heapMB()
      println(s"[GC-BATCH] Lote ${loteIdx+1}/$totalLotes | partes ${lote.head}-${lote.last} | heap=${heapAntes}MB")

      lote.foreach { partId =>
        semaphore.acquire()
        val tRead = System.currentTimeMillis()
        val rows: Array[Row] = Try {
          spark.read.format("delta").load(cfg.parquetPath)
            .filter(col(cfg.parquetPartitionCol) === partId)
            .drop(cfg.parquetPartitionCol).collect()
        }.getOrElse { semaphore.release(); Array.empty[Row] }
        println(s"[F2] READ part=$partId rows=${rows.length} READ=${System.currentTimeMillis()-tRead}ms")
        workQueue.put((partId, rows))
      }

      if (cfg.requiresCheckpoint && cfg.checkpointMode == "batch") {
        val loteMap = lote.flatMap(id => Option(batchResults.remove(id))).map(r => r.partId -> r).toMap
        if (loteMap.nonEmpty) { checkpointFlushBatch(loteMap, runDate, cfg); }
      }
      System.gc()
      println(s"[GC-BATCH] heap post-GC: ${heapMB()}MB")
      loteIdx += 1
    }

    (0 until effectiveConc).foreach(_ => workQueue.put((-1, Array.empty[Row])))
    pool.shutdown()
    pool.awaitTermination(cfg.maxPartitionTimeoutSecs.toLong * 2, java.util.concurrent.TimeUnit.SECONDS)

    val results    = { val m = scala.collection.mutable.ListBuffer.empty[BatchResult]; batchResults.forEach((_, v) => m += v); m.toList }
    val ok         = results.filter(_.success);  val failed = results.filterNot(_.success)
    val inserted   = ok.map(_.rowCount).sum
    val totalSec   = (System.currentTimeMillis() - t0) / 1000
    println(s"\n[F2 RESUMEN] ${totalSec}s | OK=${ok.size} | FAILED=${failed.size} | Filas=$inserted")

    if (cfg.requiresCheckpoint) {
      checkpointFlushBatch(results.map(r => r.partId -> r).toMap, runDate, cfg)
    }
    checkpointPrintSummary(runDate, cfg)
    if (failed.nonEmpty) throw new RuntimeException(
      s"${failed.size} particiones fallaron. Relanzar con mismo cfg.")
  }

  // ── Flujo B.3: loadFromParquetParallel — Fase 2 worker-side (Job Cluster) ─
  // Los workers leen Delta y hacen BulkCopy directamente via BulkWorker.executeBlocks.
  // Requiere Job Cluster Dedicated (sin Spark Connect → sin Task not serializable).

  def loadFromParquetParallel(numParts: Int, filteredCols: List[String],
                               runDate: java.sql.Date, cfg: AppConfig)
                              (implicit spark: SparkSession): Unit = {
    require(cfg.parquetPath.nonEmpty, "[F2-PAR] parquetPath requerido")

    val useNtile = cfg.ntileColumn.nonEmpty
    val allParts = if (useNtile) (1 to numParts).toList else (0 until numParts).toList
    val completed = checkpointGetCompleted(runDate, cfg)
    val pending   = allParts.filterNot(completed.contains)

    println(s"[F2-PAR] total=$numParts | SUCCESS=${completed.size} | pendientes=${pending.size}")
    if (pending.isEmpty) { println("[F2-PAR] Todas en SUCCESS."); return }

    val schemaJson: String = Try {
      val s = spark.read.format("delta").load(cfg.parquetPath)
        .filter(col(cfg.parquetPartitionCol) === allParts.head)
        .drop(cfg.parquetPartitionCol).limit(1).schema
      println(s"[F2-PAR] Schema: ${s.fieldNames.mkString(", ")}")
      s.json
    }.getOrElse(StructType(filteredCols.map(c => StructField(c, StringType))).json)

    val numSlots = math.max(1, math.min(pending.size, cfg.maxConcurrentPartitions * 2))
    val runId    = java.util.UUID.randomUUID().toString.take(8)
    val accOk    = spark.sparkContext.longAccumulator(s"f2par_ok_$runId")
    val accFail  = spark.sparkContext.longAccumulator(s"f2par_fail_$runId")
    val accRows  = spark.sparkContext.longAccumulator(s"f2par_rows_$runId")
    val t0       = System.currentTimeMillis()

    val pidSchema = StructType(Seq(StructField("partId", IntegerType, nullable = false)))
    val pidRows   = java.util.Arrays.asList(pending.map(id => Row(id)): _*)
    val pidDf     = spark.createDataFrame(pidRows, pidSchema).repartition(numSlots)

    BulkWorker.executeBlocks(
      blocksDf      = pidDf,
      deltaSource   = cfg.parquetPath,
      isDeltaTable  = false,
      partitionCol  = cfg.parquetPartitionCol,
      targetTable   = cfg.targetTable,
      jdbcUrl       = cfg.jdbcUrlForBulkCopy,
      filteredCols  = filteredCols,
      schemaJson    = schemaJson,
      tableLock     = cfg.initialLoadMode || cfg.tableLockOnBulk,
      bulkCopyBatch = if (cfg.initialLoadMode) 0 else cfg.bulkCopyBatchSize,
      bulkTimeout   = cfg.maxPartitionTimeoutSecs,
      blockRowLimit = cfg.blockRowLimit,
      accOk = accOk, accFail = accFail, accRows = accRows
    )

    val totalSec = (System.currentTimeMillis() - t0) / 1000
    val ok = accOk.value.toInt; val fail = accFail.value.toInt; val rows = accRows.value
    println(s"[F2-PAR] ${totalSec}s | OK=$ok | FAILED=$fail | Filas=$rows | ~${if(totalSec>0)rows/totalSec else 0} filas/s")

    if (cfg.requiresCheckpoint) {
      val bm = pending.map(pid => pid -> BatchResult(
        java.util.UUID.randomUUID().toString.take(12), pid,
        if (ok > 0) rows / math.max(1, ok) else 0L, fail == 0)).toMap
      checkpointFlushBatch(bm, runDate, cfg)
    }
    if (fail > 0) throw new RuntimeException(s"[F2-PAR] $fail particiones fallaron.")
  }


  // ── Flujo C: runFromDeltaBlocks — tabla Delta con columna BLOCK ───────────
  // El notebook aplica NTILE externamente y genera una tabla Delta con col BLOCK.
  // Este método lee cada bloque y hace BulkCopy en paralelo desde el driver.
  // Compatible con Standard y Job Cluster (driver-side: sin foreachPartition).

  def runFromDeltaBlocks(
    deltaPath    : String,
    numBlocks    : Int,
    blockCol     : String      = "BLOCK",
    filteredCols : List[String],
    cfg          : AppConfig
  )(implicit spark: SparkSession = SparkSession.active): RunResult = {

    require(deltaPath.nonEmpty && numBlocks > 0 && filteredCols.nonEmpty)
    require(blockCol.matches("[a-zA-Z_][a-zA-Z0-9_]*"),
      s"[DELTA-BLOCKS] blockCol '$blockCol' inválido")
    cfg.validateCheckpointConfig()

    val rt        = Runtime.getRuntime
    def heapMB()  = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024
    val todayDate = new java.sql.Date(System.currentTimeMillis())

    val runDate: java.sql.Date = if (cfg.requiresCheckpoint && !cfg.forceNewRun) {
      Try {
        val rows = spark.sql(s"""SELECT DISTINCT RunDate FROM ${cfg.checkpointTable}
          WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}'
            AND Status IN ('FAILED','PENDING','RUNNING')
          ORDER BY RunDate DESC LIMIT 1""").collect()
        if (rows.nonEmpty) { val d = rows(0).getDate(0); println(s"[DELTA-BLOCKS] Reanudando ($d)"); d }
        else todayDate
      }.getOrElse(todayDate)
    } else todayDate

    println("=" * 70)
    println(s" SqlServerBatchWriter.runFromDeltaBlocks | DBR 17.3 LTS | Spark 4.0")
    println(s" Delta     : $deltaPath | isDeltaTable=${cfg.isDeltaTable}")
    println(s" Bloques   : $numBlocks | columna=$blockCol")
    println(s" Target    : ${cfg.targetTable} | concurrencia=${cfg.maxConcurrentPartitions}")
    println(s" tableLock : ${cfg.tableLockOnBulk} | bulkBatch=${cfg.bulkCopyBatchSize} | blockRowLimit=${cfg.blockRowLimit}")
    if (cfg.requiresCheckpoint) println(s" Checkpoint: ${cfg.checkpointTable} | ${cfg.processName} | ${cfg.country} | $runDate")
    println("=" * 70)

    val deltaSchema: StructType = Try {
      if (cfg.isDeltaTable) spark.table(deltaPath).limit(0).schema
      else spark.read.format("delta").load(deltaPath).limit(0).schema
    }.getOrElse(throw new IllegalArgumentException(
      s"[DELTA-BLOCKS] Tabla no encontrada: $deltaPath"))

    require(deltaSchema.fieldNames.map(_.toLowerCase).contains(blockCol.toLowerCase),
      s"[DELTA-BLOCKS] Columna '$blockCol' no existe. Disponibles: ${deltaSchema.fieldNames.mkString(", ")}")

    val colsToWrite = filteredCols.filterNot(_.equalsIgnoreCase(blockCol))
    require(colsToWrite.nonEmpty)
    println(s"[DELTA-BLOCKS] Cols (${colsToWrite.size}): ${colsToWrite.mkString(", ")}")

    checkpointEnsureTable(cfg)
    val completed = checkpointGetCompleted(runDate, cfg)
    val allBlocks = (1 to numBlocks).toList
    val pending   = allBlocks.filterNot(completed.contains)

    println(s"[DELTA-BLOCKS] total=$numBlocks | SUCCESS=${completed.size} | pendientes=${pending.size}")
    if (completed.nonEmpty) println(s"[DELTA-BLOCKS] Saltando: ${completed.toSeq.sorted.mkString(", ")}")

    if (pending.isEmpty)
      return RunResult(true, numBlocks, numBlocks, 0, 0, None, runDate, cfg.processName, cfg.country)

    if (completed.isEmpty) checkpointInitRun(numBlocks, runDate, cfg)

    val partSchema: StructType = Try {
      val base = if (cfg.isDeltaTable) spark.table(deltaPath) else spark.read.format("delta").load(deltaPath)
      val s = base.filter(s"$blockCol = ${pending.head}").drop(blockCol).limit(1).schema
      println(s"[DELTA-BLOCKS] Schema: ${s.fieldNames.mkString(", ")}")
      StructType(colsToWrite.flatMap(c => s.fields.find(_.name.equalsIgnoreCase(c))))
    }.getOrElse(StructType(colsToWrite.map(c => StructField(c, StringType))))

    val t0              = System.currentTimeMillis()
    val effectiveConc   = cfg.maxConcurrentPartitions
    val semaphore       = new Semaphore(effectiveConc)
    val pool            = Executors.newFixedThreadPool(effectiveConc)
    val batchResults    = new ConcurrentHashMap[Int, BatchResult]()

    println(s"[DELTA-BLOCKS] numSlots=$effectiveConc | gcBatchSize=${cfg.gcBatchSize} | heap=${heapMB()}MB")
    println(s"[DELTA-BLOCKS] blockRowLimit=${cfg.blockRowLimit} | ${if(cfg.blockRowLimit>0) s"chunks de ${cfg.blockRowLimit} filas" else "bloque completo en 1 BulkCopy"}")

    val tableLockEff     = cfg.initialLoadMode || cfg.tableLockOnBulk
    val bulkBatchEff     = if (cfg.initialLoadMode) 0 else cfg.bulkCopyBatchSize
    val jdbcUrlBulk      = cfg.jdbcUrlForBulkCopy
    val targetTableEff   = cfg.targetTable
    val bulkTimeoutEff   = cfg.maxPartitionTimeoutSecs
    val blockRowLimitEff = cfg.blockRowLimit
    val workQueue        = new java.util.concurrent.ArrayBlockingQueue[(Int, Array[Row])](effectiveConc + 1)

    // Cola de resultados: los workers depositan aquí, el hilo principal persiste en UC.
    // Esto desacopla el flush del checkpoint del pool JDBC y evita contención.
    val resultQueue = new java.util.concurrent.LinkedBlockingQueue[BatchResult]()

    def workerTask(): Runnable = new Runnable {
      def run(): Unit = {
        var active = true
        while (active) {
          val (blockId, partRows) = workQueue.take()
          if (blockId == -1) { active = false }
          else {
            val bid   = java.util.UUID.randomUUID().toString.take(12)
            val tBulk = System.currentTimeMillis()
            // Retry por bloque ante errores de conexión transitorios (SSL reset, pool exhausted)
            val maxBlockRetries = 2
            var attempt = 0; var result: Option[BatchResult] = None
            while (result.isEmpty && attempt <= maxBlockRetries) {
              if (attempt > 0) {
                val delay = 5000L * attempt
                println(s"[DELTA-BLOCKS] Reintentando block=$blockId (intento ${attempt+1}) en ${delay/1000}s...")
                Thread.sleep(delay)
              }
              result = try {
                if (partRows.nonEmpty) {
                  val opts = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions()
                  opts.setBatchSize(bulkBatchEff); opts.setCheckConstraints(false)
                  opts.setTableLock(tableLockEff); opts.setBulkCopyTimeout(bulkTimeoutEff)

                  if (blockRowLimitEff <= 0) {
                    val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrlBulk)
                    try {
                      bc.setBulkCopyOptions(opts); bc.setDestinationTableName(targetTableEff)
                      colsToWrite.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
                      bc.writeToServer(new SparkRowBulkRecord(partRows, partSchema, colsToWrite))
                    } finally { scala.util.Try(bc.close()) }
                    val ms = System.currentTimeMillis() - tBulk
                    println(s"[DELTA-BLOCKS] ✓ block=$blockId rows=${partRows.length} BULK=${ms}ms")
                    Some(BatchResult(bid, blockId, partRows.length.toLong, success = true))
                  } else {
                    var totalSent = 0L; var chunkErr: Option[String] = None
                    partRows.grouped(blockRowLimitEff).takeWhile(_ => chunkErr.isEmpty).foreach { chunk =>
                      val bc = new com.microsoft.sqlserver.jdbc.SQLServerBulkCopy(jdbcUrlBulk)
                      try {
                        bc.setBulkCopyOptions(opts); bc.setDestinationTableName(targetTableEff)
                        colsToWrite.zipWithIndex.foreach { case (c, i) => bc.addColumnMapping(i+1, c) }
                        bc.writeToServer(new SparkRowBulkRecord(chunk, partSchema, colsToWrite))
                        totalSent += chunk.length.toLong
                      } catch {
                        case ex: Throwable => scala.util.Try(bc.close()); chunkErr = Some(ex.getMessage)
                      } finally { scala.util.Try(bc.close()) }
                    }
                    val ms = System.currentTimeMillis() - tBulk
                    chunkErr match {
                      case None =>
                        println(s"[DELTA-BLOCKS] ✓ block=$blockId rows=$totalSent BULK=${ms}ms")
                        Some(BatchResult(bid, blockId, totalSent, success = true))
                      case Some(err) =>
                        throw new RuntimeException(err)
                    }
                  }
                } else Some(BatchResult(bid, blockId, 0L, success = true))
              } catch {
                case ex: Throwable =>
                  attempt += 1
                  if (attempt > maxBlockRetries) {
                    val ms = System.currentTimeMillis() - tBulk
                    println(s"[DELTA-BLOCKS] ✗ block=$blockId BULK=${ms}ms: ${ex.getMessage}")
                    Some(BatchResult(bid, blockId, 0L, success = false, errorMsg = ex.getMessage))
                  } else None
              }
            }
            result.foreach { r =>
              batchResults.put(blockId, r)
              resultQueue.put(r) // notifica al hilo principal para flush inmediato
            }
            semaphore.release()
          }
        }
      }
    }

    (0 until effectiveConc).foreach(_ => pool.submit(workerTask()))

    val lotes      = pending.grouped(math.max(1, cfg.gcBatchSize)).toList
    val totalLotes = lotes.size
    println(s"[DELTA-BLOCKS] ${totalLotes} lote(s) de GC")

    var loteIdx = 0
    while (loteIdx < totalLotes) {
      val lote      = lotes(loteIdx)
      val heapAntes = heapMB()
      println(s"[GC-BATCH] Lote ${loteIdx+1}/$totalLotes | bloques ${lote.head}-${lote.last} | heap=${heapAntes}MB")

      lote.foreach { blockId =>
        semaphore.acquire()
        val tRead = System.currentTimeMillis()
        val rows: Array[Row] = Try {
          val base = if (cfg.isDeltaTable) spark.table(deltaPath) else spark.read.format("delta").load(deltaPath)
          base.filter(col(blockCol) === blockId).drop(blockCol)
            .select(colsToWrite.map(c => col(c)): _*).collect()
        }.getOrElse { semaphore.release(); Array.empty[Row] }
        println(s"[DELTA-BLOCKS] READ block=$blockId rows=${rows.length} READ=${System.currentTimeMillis()-tRead}ms")
        workQueue.put((blockId, rows))
      }

      // Drena la cola de resultados completados y persiste en UC individualmente.
      // Cada bloque completado → UPDATE inmediato en checkpoint → idempotente en reintento.
      if (cfg.requiresCheckpoint) {
        var r = resultQueue.poll()
        while (r != null) {
          checkpointUpdateBlock(r, runDate, cfg)
          r = resultQueue.poll()
        }
      }

      System.gc()
      println(s"[GC-BATCH] heap post-GC: ${heapMB()}MB")
      loteIdx += 1
    }

    (0 until effectiveConc).foreach(_ => workQueue.put((-1, Array.empty[Row])))
    pool.shutdown()
    pool.awaitTermination(cfg.maxPartitionTimeoutSecs.toLong * 2, java.util.concurrent.TimeUnit.SECONDS)

    // Flush final: cualquier resultado que completó mientras esperábamos awaitTermination
    if (cfg.requiresCheckpoint) {
      var r = resultQueue.poll()
      while (r != null) { checkpointUpdateBlock(r, runDate, cfg); r = resultQueue.poll() }
    }

    val totalSec    = (System.currentTimeMillis() - t0) / 1000
    val remaining   = { val m = scala.collection.mutable.ListBuffer.empty[BatchResult]; batchResults.forEach((_, v) => m += v); m.toList }
    val okFinal     = remaining.count(_.success)
    val failFinal   = remaining.count(!_.success)
    val rowsFinal   = remaining.filter(_.success).map(_.rowCount).sum
    println(s"\n[DELTA-BLOCKS RESUMEN] ${totalSec}s | OK=$okFinal | FAILED=$failFinal | Filas=$rowsFinal | ~${if(totalSec>0)rowsFinal/totalSec else 0} filas/s")

    if (cfg.requiresCheckpoint) {
      val finalMap = allBlocks.map { bid =>
        val r = remaining.find(_.partId == bid)
        bid -> BatchResult(java.util.UUID.randomUUID().toString.take(12), bid,
          r.map(_.rowCount).getOrElse(0L), r.map(_.success).getOrElse(completed.contains(bid)),
          r.flatMap(x => if(x.success) None else Some(x.errorMsg)).getOrElse(""))
      }.toMap
      checkpointFlushBatch(finalMap, runDate, cfg)
    }
    checkpointPrintSummary(runDate, cfg)

    val failedEnUC = Try {
      spark.sql(s"""SELECT COUNT(*) FROM ${cfg.checkpointTable}
        WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}'
          AND RunDate=DATE '$runDate' AND Status='FAILED'""").collect()(0).getLong(0).toInt
    }.getOrElse(failFinal)

    RunResult(failedEnUC == 0, numBlocks, numBlocks - failedEnUC, failedEnUC, 0, None, runDate, cfg.processName, cfg.country)
  }

  def runFromDeltaBlocksWithRetry(
    deltaPath    : String,
    numBlocks    : Int,
    blockCol     : String      = "BLOCK",
    filteredCols : List[String],
    cfg          : AppConfig,
    maxRetries   : Int         = -1
  )(implicit spark: SparkSession = SparkSession.active): RunResult = {

    val retries = if (maxRetries >= 0) maxRetries else cfg.maxAutoRetries
    println(s"[RETRY] runFromDeltaBlocksWithRetry | maxRetries=$retries")

    var last = Try(runFromDeltaBlocks(deltaPath, numBlocks, blockCol, filteredCols, cfg)) match {
      case scala.util.Success(r) => r
      case scala.util.Failure(ex) =>
        println(s"[RETRY] ERROR: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        ex.getStackTrace.take(3).foreach(f => println(s"  at $f"))
        RunResult(false, numBlocks, 0, numBlocks, 0, Some(ex.getMessage),
          new java.sql.Date(System.currentTimeMillis()), cfg.processName, cfg.country)
    }

    if (!last.success && last.successParts == 0 && last.failedParts == numBlocks) {
      println(s"[RETRY] Error no recuperable — revisar conexión, tabla y columna '$blockCol'")
      last.printSummary(); return last
    }

    var attempt = 1
    while (last.needsRetry && attempt <= retries) {
      val delayMs = math.min(30_000L, 5_000L * attempt)
      println(s"[RETRY] Esperando ${delayMs/1000}s... intento $attempt/$retries | FAILED=${last.failedParts}")
      Thread.sleep(delayMs)
      last = Try(runFromDeltaBlocks(deltaPath, numBlocks, blockCol, filteredCols, cfg.copy(forceNewRun = false))) match {
        case scala.util.Success(r) => r
        case scala.util.Failure(ex) =>
          println(s"[RETRY] ERROR reintento $attempt: ${ex.getMessage}")
          last.copy(errorMessage = Some(ex.getMessage))
      }
      attempt += 1
    }

    if (last.isComplete) println(s"[RETRY] ✓ Completado en $attempt intento(s).")
    else                  println(s"[RETRY] ✗ Agotados $retries reintentos. FAILED=${last.failedParts}")
    last.printSummary(); last
  }


  // ── Punto de entrada principal: runBatchLoad ──────────────────────────────
  // Selecciona flujo automáticamente según volumen:
  //   < thresholdXLarge : writeDirectBulk (workers Spark, sin Delta)
  //   >= thresholdXLarge: Parquet Landing (Fase 1 + Fase 2)

  def runBatchLoad(inputDf: DataFrame, cfg: AppConfig)
                  (implicit spark: SparkSession = inputDf.sparkSession): RunResult = {
    cfg.validateCheckpointConfig()

    val totalRows = inputDf.count()
    val useParquet = cfg.forceParquetLanding.getOrElse(totalRows >= cfg.thresholdXLarge)

    println(s"[RUN] ${cfg.targetTable} | filas=$totalRows | modo=${if(useParquet) "PARQUET LANDING" else "JDBC DIRECTO"}")

    val excludeSet   = cfg.excludeColumns.map(_.toLowerCase).toSet
    val filteredCols = inputDf.columns.filterNot(c => excludeSet.contains(c.toLowerCase)).toList
    val todayDate    = new java.sql.Date(System.currentTimeMillis())

    val runDate: java.sql.Date = if (cfg.requiresCheckpoint && !cfg.forceNewRun) {
      Try {
        val rows = spark.sql(s"""SELECT DISTINCT RunDate FROM ${cfg.checkpointTable}
          WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}'
            AND Status IN ('FAILED','PENDING','RUNNING')
          ORDER BY RunDate DESC LIMIT 1""").collect()
        if (rows.nonEmpty) rows(0).getDate(0) else todayDate
      }.getOrElse(todayDate)
    } else todayDate

    if (!useParquet) {
      Try(writeDirectBulk(inputDf, totalRows, cfg)) match {
        case scala.util.Success(_) =>
          RunResult(true, 1, 1, 0, 0, None, runDate, cfg.processName, cfg.country)
        case scala.util.Failure(ex) =>
          println(s"[DIRECT] ERROR: ${ex.getMessage}")
          ex.getStackTrace.take(4).foreach(f => println(s"  at $f"))
          RunResult(false, 1, 0, 1, 0, Some(ex.getMessage), runDate, cfg.processName, cfg.country)
      }
    } else {
      checkpointEnsureTable(cfg)
      val numParts = math.max(1, math.ceil(totalRows.toDouble / cfg.batchSize).toInt)
      val skipF1   = !cfg.forceNewRun && Try {
        spark.read.format("delta").load(cfg.parquetPath).limit(1).count() >= 0
      }.getOrElse(false) && checkpointGetCompleted(runDate, cfg).nonEmpty

      if (!skipF1) {
        println(s"[RUN] Fase 1: $totalRows filas → $numParts particiones → ${cfg.parquetPath}")
        writeToParquet(inputDf, totalRows, cfg)
      } else {
        println("[RUN] Saltando Fase 1 — Delta existente y checkpoint con progreso")
      }

      println(s"[RUN] Fase 2: ${if(cfg.useParallelLoad) "WORKER-SIDE (Job Cluster)" else "DRIVER-SIDE (Standard)"}")
      Try {
        if (cfg.useParallelLoad) loadFromParquetParallel(numParts, filteredCols, runDate, cfg)
        else                     loadFromParquet(numParts, filteredCols, runDate, cfg)
      } match {
        case scala.util.Success(_) =>
          RunResult(true, numParts, numParts, 0, 0, None, runDate, cfg.processName, cfg.country)
        case scala.util.Failure(ex) =>
          val ckCompleted = Try(checkpointGetCompleted(runDate, cfg).size).getOrElse(0)
          RunResult(false, numParts, ckCompleted, numParts - ckCompleted, 0,
            Some(ex.getMessage), runDate, cfg.processName, cfg.country)
      }
    }
  }

  def runBatchLoadWithRetry(inputDf: DataFrame, cfg: AppConfig)
                            (implicit spark: SparkSession = inputDf.sparkSession): RunResult = {
    val retries = math.max(0, cfg.maxAutoRetries)
    println(s"[RETRY] runBatchLoadWithRetry | maxRetries=$retries")

    var result = Try(runBatchLoad(inputDf, cfg)) match {
      case scala.util.Success(r) => r
      case scala.util.Failure(ex) =>
        println(s"[RETRY] ERROR: ${ex.getMessage}")
        RunResult(false, 0, 0, 0, 0, Some(ex.getMessage))
    }

    var attempt = 1
    while (result.needsRetry && attempt <= retries) {
      println(s"[RETRY] Reintento $attempt/$retries | FAILED=${result.failedParts}")
      Thread.sleep(math.min(30_000L, 5_000L * attempt))
      result = Try(runBatchLoad(inputDf, cfg.copy(forceNewRun = false))) match {
        case scala.util.Success(r) => r
        case scala.util.Failure(ex) => result.copy(errorMessage = Some(ex.getMessage))
      }
      attempt += 1
    }

    result.printSummary(); result
  }


  // ── Checkpoint Unity Catalog ──────────────────────────────────────────────

  def checkpointEnsureTable(cfg: AppConfig)(implicit spark: SparkSession): Unit = {
    if (!cfg.requiresCheckpoint) return
    spark.sql(s"""CREATE TABLE IF NOT EXISTS ${cfg.checkpointTable} (
      ProcessName STRING, Country STRING, RunDate DATE, PartitionId INT,
      Status STRING, RowCount LONG, RetryCount INT, ErrorMessage STRING,
      LastUpdate TIMESTAMP
    ) USING DELTA""")
    println(s"[UC CHECKPOINT] Tabla lista: ${cfg.checkpointTable}")
  }

  def checkpointInitRun(numParts: Int, runDate: java.sql.Date, cfg: AppConfig)
                       (implicit spark: SparkSession): Unit = {
    if (!cfg.requiresCheckpoint) return
    val existing = checkpointGetCompleted(runDate, cfg).size
    if (existing == numParts) return
    val vals = ((existing + 1) to numParts).map { i =>
      s"('${cfg.processName}','${cfg.country}',DATE '$runDate',$i,'PENDING',0,0,'',current_timestamp())"
    }.mkString(",")
    if (vals.nonEmpty) {
      spark.sql(s"INSERT INTO ${cfg.checkpointTable} VALUES $vals")
      println(s"[UC CHECKPOINT] Inicializando ${numParts - existing} particiones como PENDING ($existing ya en SUCCESS).")
    }
  }

  // UPDATE individual de 1 bloque — llamado inmediatamente al completar cada bloque.
  // El registro ya existe como PENDING (creado en checkpointInitRun), solo se actualiza.
  // UPDATE es ~3× más rápido que MERGE para Delta Lake y garantiza idempotencia:
  // si el bloque está en SUCCESS, el reintento lo saltará y no habrá duplicados.
  def checkpointUpdateBlock(r: BatchResult, runDate: java.sql.Date, cfg: AppConfig)
                            (implicit spark: SparkSession): Unit = {
    if (!cfg.requiresCheckpoint) return
    val status  = if (r.success) "SUCCESS" else "FAILED"
    val errSafe = r.errorMsg.replace("'", "''").take(500)
    Try {
      spark.sql(s"""UPDATE ${cfg.checkpointTable}
        SET Status='$status', RowCount=${r.rowCount}, ErrorMessage='$errSafe',
            LastUpdate=current_timestamp()
        WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}'
          AND RunDate=DATE '$runDate' AND PartitionId=${r.partId}""")
      if (r.success) println(s"[UC CHECKPOINT] block=${r.partId} → SUCCESS (${r.rowCount} filas)")
    }.recover { case ex => println(s"[UC CHECKPOINT] WARN: no se pudo persistir block=${r.partId}: ${ex.getMessage}") }
  }

  def checkpointFlushBatch(results: Map[Int, BatchResult], runDate: java.sql.Date, cfg: AppConfig)
                           (implicit spark: SparkSession): Unit = {
    if (!cfg.requiresCheckpoint || results.isEmpty) return
    // Flush masivo al final de la ejecución — UPDATE por lote via SELECT UNION ALL.
    // Los registros ya existen (creados como PENDING en checkpointInitRun).
    val rows = results.values.map { r =>
      val status  = if (r.success) "SUCCESS" else "FAILED"
      val errSafe = r.errorMsg.replace("'", "''").take(500)
      s"""SELECT '${cfg.processName}' AS ProcessName,'${cfg.country}' AS Country,
              DATE '$runDate' AS RunDate,${r.partId} AS PartitionId,'$status' AS Status,
              ${r.rowCount} AS RowCount,0 AS RetryCount,'$errSafe' AS ErrorMessage,
              current_timestamp() AS LastUpdate"""
    }.mkString(" UNION ALL ")
    spark.sql(s"""MERGE INTO ${cfg.checkpointTable} AS t
      USING ($rows) AS s
      ON t.ProcessName=s.ProcessName AND t.Country=s.Country
         AND t.RunDate=s.RunDate AND t.PartitionId=s.PartitionId
      WHEN MATCHED THEN UPDATE SET
        t.Status=s.Status, t.RowCount=s.RowCount,
        t.ErrorMessage=s.ErrorMessage, t.LastUpdate=s.LastUpdate
      WHEN NOT MATCHED THEN INSERT *""")
  }

  def checkpointGetCompleted(runDate: java.sql.Date, cfg: AppConfig)
                             (implicit spark: SparkSession): Set[Int] = {
    if (!cfg.requiresCheckpoint) return Set.empty
    Try {
      spark.sql(s"""SELECT PartitionId FROM ${cfg.checkpointTable}
        WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}'
          AND RunDate=DATE '$runDate' AND Status='SUCCESS'""")
        .collect().map(_.getInt(0)).toSet
    }.getOrElse(Set.empty)
  }

  def checkpointPrintSummary(runDate: java.sql.Date, cfg: AppConfig)
                              (implicit spark: SparkSession): Unit = {
    if (!cfg.requiresCheckpoint) return
    Try {
      spark.sql(s"""SELECT Status, COUNT(*) Partes, SUM(RowCount) Filas
        FROM ${cfg.checkpointTable}
        WHERE ProcessName='${cfg.processName}' AND Country='${cfg.country}' AND RunDate=DATE '$runDate'
        GROUP BY Status ORDER BY Status""").show(truncate = false)
    }
  }

  // ── Stored Procedures ────────────────────────────────────────────────────

  case class SpResult(success: Boolean, returnCode: Int = 0,
    outputParams: Map[String, Any] = Map.empty,
    resultSets: List[List[Map[String, Any]]] = Nil,
    errorMessage: Option[String] = None, durationMs: Long = 0L)

  def executeStoredProc(procName: String, params: List[SpParam] = Nil,
                         cfg: AppConfig, timeoutSecs: Int = 300,
                         maxRetries: Int = -1): SpResult = {
    val retries = if (maxRetries >= 0) maxRetries else cfg.spMaxRetries
    var attempt = 0; var lastErr = ""
    while (attempt <= retries) {
      val t0 = System.currentTimeMillis()
      val spTimeoutSecs = if (cfg.spSocketTimeoutSecs > 0) cfg.spSocketTimeoutSecs else timeoutSecs + 60
      val spUrl = {
        val base = if (cfg.jdbcUrl.endsWith(";")) cfg.jdbcUrl else cfg.jdbcUrl + ";"
        base + s"user=${cfg.jdbcUser};password=${cfg.jdbcPassword};loginTimeout=30;" +
               s"socketTimeout=${spTimeoutSecs * 1000};connectRetryCount=2;connectRetryInterval=5;"
      }
      val conn = Try(DriverManager.getConnection(spUrl))
      conn match {
        case scala.util.Failure(ex) =>
          lastErr = ex.getMessage; attempt += 1
          if (attempt <= retries) Thread.sleep(cfg.spRetryDelayMs * attempt)
        case scala.util.Success(c) =>
          try {
            val placeholders = params.map(_ => "?").mkString(",")
            val sql = s"{? = CALL $procName($placeholders)}"
            val cs  = c.prepareCall(sql)
            cs.setQueryTimeout(timeoutSecs)
            cs.registerOutParameter(1, Types.INTEGER)
            params.zipWithIndex.foreach { case (p, i) =>
              val pos = i + 2
              p.value.foreach { v =>
                v match {
                  case s: String  => cs.setString(pos, s)
                  case n: Int     => cs.setInt(pos, n)
                  case n: Long    => cs.setLong(pos, n)
                  case b: Boolean => cs.setBoolean(pos, b)
                  case d: java.sql.Date => cs.setDate(pos, d)
                  case _          => cs.setObject(pos, v)
                }
              }
              if (p.isOutput) p.sqlType.foreach(t => cs.registerOutParameter(pos, t))
            }
            val hasResults = cs.execute()
            val rc = cs.getInt(1)
            val outMap = params.zipWithIndex.collect { case (p, i) if p.isOutput =>
              p.name -> cs.getObject(i + 2).asInstanceOf[Any]
            }.toMap
            val sets = if (hasResults) {
              val buf = scala.collection.mutable.ListBuffer.empty[List[Map[String, Any]]]
              var rs: ResultSet = cs.getResultSet
              while (rs != null) {
                val md  = rs.getMetaData
                val rows = scala.collection.mutable.ListBuffer.empty[Map[String, Any]]
                while (rs.next()) rows += (1 to md.getColumnCount).map(j => md.getColumnName(j) -> rs.getObject(j).asInstanceOf[Any]).toMap
                buf += rows.toList
                rs = if (cs.getMoreResults()) cs.getResultSet else null
              }
              buf.toList
            } else Nil
            return SpResult(true, rc, outMap, sets, None, System.currentTimeMillis() - t0)
          } catch {
            case ex: Throwable =>
              lastErr = ex.getMessage; attempt += 1
              if (attempt <= retries) Thread.sleep(cfg.spRetryDelayMs * attempt)
          } finally { Try(c.close()) }
      }
    }
    SpResult(false, -1, Map.empty, Nil, Some(lastErr))
  }

} // fin object SqlServerBatchWriter