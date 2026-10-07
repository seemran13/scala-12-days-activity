package day23

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.File

/**
 * Day 23 — DStreams Basics (companion: file-based text stream)
 *
 * SocketLogStreamApp covers the SOCKET half of "read a socket/text stream".
 * This smaller app covers the FILE half: textFileStream monitors a
 * directory, and every NEW file dropped into it (after the stream starts)
 * becomes part of the next micro-batch. No socket or server process needed -
 * just copy or write files into ./streaming_input/ while this runs.
 *
 * Run with (note runMain - this file has its own main, separate from
 * SocketLogStreamApp's, so `sbt run` alone would ask which one you meant):
 *
 *   sbt "runMain day23.FileLogStreamApp"
 *
 * Then, in a SECOND terminal, from this project's root, either:
 *   - drop a file in by hand:
 *       cp src/main/resources/sample_app_logs.txt streaming_input/batch1.txt
 *   - or let the feeder script do it automatically every few seconds:
 *       python3 scripts/file_stream_feeder.py
 */
object FileLogStreamApp {

  val watchDir = "streaming_input"

  def main(args: Array[String]): Unit = {
    val dir = new File(watchDir)
    if (!dir.exists()) dir.mkdirs()

    val conf = new SparkConf().setAppName("Day23-FileLogStreamApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    println(s"Watching ./$watchDir/ for NEW files, checked once per 5-second batch interval.")
    println("textFileStream only picks up files that appear AFTER the stream starts - anything")
    println("already sitting in the folder when this line runs is silently ignored.\n")

    // textFileStream reads whole NEW text files as they appear in a
    // directory - "a text stream" in the file sense, as opposed to
    // socketTextStream's line-by-line TCP connection. Each batch interval,
    // Spark lists the directory, finds files newer than the last check, and
    // turns their combined lines into that interval's RDD. Spark expects
    // each file to appear ATOMICALLY (fully written, then moved/renamed
    // into place) - a file still being written to when the directory is
    // scanned can be read incompletely, which is why the feeder script
    // writes to a .tmp name and renames it into place at the end.
    val lines = ssc.textFileStream(watchDir)

    val nonBlank = lines.filter(_.trim.nonEmpty)
    val words = nonBlank.flatMap(_.split("\\s+")).filter(_.nonEmpty)
    val errorLines = nonBlank.filter(_.toUpperCase.contains("ERROR"))

    nonBlank.foreachRDD { (rdd, time) =>
      val count = rdd.count()
      if (count > 0) println(s"[file stream] batch @ $time -> $count new log line(s) read from new file(s)")
    }
    words.foreachRDD { (rdd, _) =>
      val count = rdd.count()
      if (count > 0) println(s"                 word count this batch : $count")
    }
    errorLines.foreachRDD { (rdd, _) =>
      val count = rdd.count()
      if (count > 0) println(s"                 ERROR lines this batch: $count")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
