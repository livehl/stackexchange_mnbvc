import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.rocksdb.{CompressionType, FlushOptions, Options, RocksDB, WriteBatch, WriteOptions}

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream, InputStream, PrintWriter, StringWriter}
import javax.xml.stream._
import java.util
import scala.collection.mutable

object AllSiteClass {
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  val save_path="jsonl/"

  val convertMap = Map("&quot;" -> "\"", "&amp;" -> "&", "&lt;" -> "<", "&gt;" -> ">", "&nbsp;" -> " ", "&#xA;" -> "\\n")

  def toJson(value: AnyRef) = {
    val out = new StringWriter()
    mapper.writeValue(out, value)
    out.toString()
  }

  def jsonToMap(value: String) = {
    mapper.readValue(value, classOf[Map[String, Any]])
  }

  def convertXml(fis: InputStream, dbName: String): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
        .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
        .setTargetFileSizeBase(1024 * 1024 * 1024)
      val db = RocksDB.open(options, dbName)

      val factory = XMLInputFactory.newInstance()
      val reader = factory.createXMLStreamReader(fis)
      var count: Int = 0
      var ts: Long = System.currentTimeMillis()
      val all_lines: Int = 647
      var has_all: Int = 0
      while (reader.hasNext) {
        val event = reader.next()
        event match {
          case XMLStreamConstants.START_ELEMENT =>
            val map = 0.until(reader.getAttributeCount).map(i => reader.getAttributeLocalName(i) -> reader.getAttributeValue(i)).toMap
            if (map.size > 0) {
              val json_value = toJson(map)
              db.put(map("Id").getBytes("utf-8"), json_value.getBytes("utf-8"))
              count += 1
              has_all += 1
              if (count >= 100000) {
                println(s"append 10000,has ${has_all / 10000}w ,progress ${(has_all / all_lines)}% use time :${System.currentTimeMillis() - ts}")
                ts = System.currentTimeMillis()
                count = 0
              }
            }

          case _ =>

        }
      }
      db.flush(new FlushOptions().setWaitForFlush(true))
      db.close()

      reader.close()
      println("end")

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  def readFile(file: String) = {
    val f = new File(file)
    val sevenZFile = new SevenZFile(f)
    var entry = sevenZFile.getNextEntry()
    var db = ""
    while (entry != null) {
      if (entry.getName() == "Posts.xml") {
        println(f.getName, entry.getName())
        db = f.getName.split("\\.").head + ".db"
        convertXml(sevenZFile.getInputStream(entry), db)
      }
      entry = sevenZFile.getNextEntry()
    }
    sevenZFile.close()
    db
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }


  def main(args: Array[String]): Unit = {
      new File("F:\\迅雷下载\\stackexchange").listFiles.foreach(fs=>dealSingleFile(fs.toString))

  }

  def dealSingleFile(file: String) = {
    println(file)
    val db = readFile(file)
    dbCompact(db)
    save_question_to_db(db)
    cleanData(db)
    deleteRecursively(new File(db))
    deleteRecursively(new File("question_" + db))
  }

  def dbCompact(db: String): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
        .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
        .setTargetFileSizeBase(1024 * 1024 * 1024)
      val opendb = RocksDB.open(options, db)
      opendb.compactRange()
      opendb.flush(new FlushOptions().setWaitForFlush(true))
      opendb.close()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def save_question_to_db(dbName: String): Unit = {
    RocksDB.loadLibrary()
    val options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
    val db = RocksDB.open(options, dbName)

    val write_options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
      .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
      .setTargetFileSizeBase(1024 * 1024 * 1024)
    val question_db = RocksDB.open(write_options, "question_" + dbName)

    val iterator = db.newIterator()
    iterator.seekToFirst()
    while (iterator.isValid) {
      val data = iterator.value()
      val map_data = jsonToMap(new String(data, "utf-8"))
      if (map_data("PostTypeId").asInstanceOf[String].toInt == 1) {
        val body = convert_html_character(map_data("Body").asInstanceOf[String])
        question_db.put(iterator.key(), toJson(map_data + ("Body" -> body)).getBytes("utf-8"))
      }
      iterator.next()
    }
    iterator.close()
    db.close()
    question_db.flush(new FlushOptions().setWaitForFlush(true))
    question_db.close()
    println("end")

  }

  def cleanData(dbName: String): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
      val db = RocksDB.open(options, dbName)
      val question_db = RocksDB.open(options, "question_" + dbName)
      val iterator = db.newIterator()
      iterator.seekToFirst()
      var count: Int = 0
      var ts: Long = System.currentTimeMillis()
      val all_lines: Int = 647
      var has_all: Int = 0
      var file_num = 1
      val has_ids = mutable.HashSet[Int]()
      var json_len = 0
      val json_list = mutable.ListBuffer[String]()
      val path_name = save_path+dbName.replace(".db", "")
      new File(path_name).mkdirs()
      while (iterator.isValid) { //遍历所有答案
        val data = new String(iterator.value(), "utf-8")
        val map_data = jsonToMap(data)
        if (map_data("PostTypeId").asInstanceOf[String].toInt == 2) {
          count += 1
          val question = new String(question_db.get(map_data("ParentId").asInstanceOf[String].getBytes("utf-8")), "utf-8")
          val question_map = jsonToMap(question)
          val question_body = convert_html_character(question_map("Body").asInstanceOf[String])
          val body = convert_html_character(map_data("Body").asInstanceOf[String])
          val json_str = get_json(question_map + ("Body" -> question_body), map_data + ("Body" -> body), question, data, path_name + ".stackexchange.com")
          json_list.append(json_str)
          json_len += json_str.getBytes("utf-8").size
          has_ids.add(question_map("Id").asInstanceOf[String].toInt)
          if (json_len >= 500 * 1024 * 1024) {
            val pw = new PrintWriter(s"${path_name}/${file_num}.jsonl", "utf-8")
            json_list.foreach(s => pw.println(s))
            pw.flush()
            pw.close()
            json_list.clear()
            json_len = 0
            file_num += 1
            println(s"write new file $file_num")
          }
        }
        iterator.next()
      }
      //写入没有满的数据
      if (json_len >= 0) {
        val pw = new PrintWriter(s"${path_name}/${file_num}.jsonl", "utf-8")
        json_list.foreach(s => pw.println(s))
        pw.flush()
        pw.close()
        json_list.clear()
        json_len = 0
        file_num += 1
        println(s"write new file $file_num")
      }
      file_num = 1000
      val question_iterator = question_db.newIterator()
      question_iterator.seekToFirst()
      while (question_iterator.isValid) { //遍历所有问题
        val question = new String(question_iterator.value(), "utf-8")
        val question_map = jsonToMap(question)
        if (!has_ids.contains(question_map("Id").asInstanceOf[String].toInt)) {
          val question_body = convert_html_character(question_map("Body").asInstanceOf[String])
          val json_str = get_json(question_map + ("Body" -> question_body), Map.empty, question, "", path_name + ".stackexchange.com")
          json_list.append(json_str)
          json_len += json_str.getBytes("utf-8").size
          has_ids.add(question_map("Id").asInstanceOf[String].toInt)
          if (json_len >= 500 * 1024 * 1024) {
            val pw = new PrintWriter(s"${path_name}/${file_num}.jsonl", "utf-8")
            json_list.foreach(s => pw.println(s))
            pw.flush()
            pw.close()
            json_list.clear()
            json_len = 0
            file_num += 1
            println(s"write new file $file_num")
          }
        }
        question_iterator.next()
      }
      //写入没有满的数据
      if (json_len >= 0) {
        val pw = new PrintWriter(s"${path_name}/${file_num}.jsonl", "utf-8")
        json_list.foreach(s => pw.println(s))
        pw.flush()
        pw.close()
        json_list.clear()
        json_len = 0
        file_num += 1
        println(s"write new file $file_num")
      }

      iterator.close()
      question_iterator.close()
      db.close()
      question_db.close()
      println("end")

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 替换所有的html标签
   *
   * @param str
   * @return
   */
  def convert_html_character(str: String) = {
    val ts = System.currentTimeMillis()
    var html = convertMap.foldLeft(str) { (ostr, kv) => ostr.replace(kv._1, kv._2) }
    if (html.startsWith("<p>")) { //去除头尾的p标签
      html = html.drop(3)
    }
    if (html.endsWith("</p>\\n")) {
      html = html.dropRight(6) + "\\n"
    }

    if (html.contains("<a")) {
      var html_deal = html
      val cut_pre_html = mutable.HashMap[String, String]()
      var num = 0
      while (html_deal.contains("<pre>")) { //提取pre段
        val idx = html_deal.indexOf("<pre>")
        val idx_end = html_deal.indexOf("</pre>", idx)
        if (idx_end <= 0) {
          cut_pre_html(s"replace_pre_$num") = "<pre>"
          html_deal = html_deal.take(idx) + s"replace_pre_$num" + html_deal.drop(idx + 5)
        } else {
          val html_cut = html_deal.drop(idx).take(idx_end - idx + 6)
          cut_pre_html(s"replace_pre_$num") = html_cut
          html_deal = html_deal.take(idx) + s"replace_pre_$num" + html_deal.drop(idx_end + 6)
        }
        num += 1
        if (System.currentTimeMillis() - ts > 1000) {
          println("error")
        }
      }
      val cut_a_html = mutable.HashMap[String, String]()
      val html_sb = new StringBuilder(html_deal)
      while (html_sb.indexOf("<a ") != -1) { //提取非pre段的 a标签。如果没有body，则不处理
        val idx = html_sb.indexOf("<a ")
        val idx_over = html_sb.indexOf(">", idx)
        val idx_end = html_sb.indexOf("</a>", idx_over)
        if (idx_over == -1 || idx_end == -1) { //非标准的a标签
          cut_a_html(s"replace_pre_$num") = "<a "
          num += 1
          html_sb.delete(idx, idx + 3)
        } else {
          html_sb.delete(idx_end, idx_end + 4)
          html_sb.delete(idx, idx_over + 1)
        }
        if (System.currentTimeMillis() - ts > 1000) {
          println("error")
        }
      }
      //提取a链接的text
      cut_pre_html.foreach(kv => replaceAll(html_sb, kv._1, kv._2))
      cut_a_html.foreach(kv => replaceAll(html_sb, kv._1, kv._2))
      html = html_sb.toString()
    }
    html.replace("<pre>", "```").replace("</pre>", "```")
  }

  def replaceAll(builder: StringBuilder, from: String, to: String) {
    var index = builder.indexOf(from);
    while (index != -1) {
      builder.replace(index, index + from.length(), to);
      index += to.length();
      index = builder.indexOf(from, index);
    }
  }

  def convert_date(str: String) = {
    val r_str = str.replace("-", "").replace("T", " ")
    r_str.take(r_str.indexOf("."))
  }

  def get_json(question: Map[String, Any], data: Map[String, Any], question_str: String, data_str: String, source: String) = {
    toJson(Map(
      "id" -> question("Id"),
      "问" -> (question("Title").asInstanceOf[String] + "\n" + question("Body").asInstanceOf[String])
      , "答" -> data.getOrElse("Body", "")
      , "来源" -> source
      , "元数据" -> Map(
        "create_time" -> convert_date(data.getOrElse("CreationDate", question("CreationDate")).asInstanceOf[String])
        ,
        "问题明细" -> question_str
        ,
        "回答明细" -> data_str
        ,
        "扩展字段" -> Map("问点赞数" -> question("Score"), "答点赞数" -> data.getOrElse("Score", "0"))
      )
    ))
  }
}