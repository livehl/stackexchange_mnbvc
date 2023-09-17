import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.rocksdb.{CompressionType, FlushOptions, Options, RocksDB, WriteBatch, WriteOptions}

import java.io.{File, FileInputStream, PrintWriter, StringWriter}
import javax.xml.stream._
import java.util
import scala.collection.mutable


object mainClass {
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  val convertMap = Map("&quot;" -> "\"", "&amp;" -> "&", "&lt;" -> "<", "&gt;" -> ">", "&nbsp;" -> " ", "&#xA;" -> "\\n")

  def toJson(value: AnyRef) = {
    val out = new StringWriter()
    mapper.writeValue(out, value)
    out.toString()
  }

  def jsonToMap(value: String) = {
    mapper.readValue(value, classOf[Map[String, Any]])
  }

  def convertXml: Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
        .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
        .setTargetFileSizeBase(1024 * 1024 * 1024)
      val db = RocksDB.open(options, "tags.db")

      val factory = XMLInputFactory.newInstance()
      val fs = new FileInputStream(new File("F:/stackexchange/Tags.xml"))
      val reader = factory.createXMLStreamReader(fs)
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

  def main(args: Array[String]): Unit = {
//    val body = """<p>I like <pre> <a href=\"http://www.chmaas.handshake.de/delphi/freeware/xvi32/xvi32.htm\" rel=\"noreferrer\">12312312</a> </pre>, although it seems similar to the above  <a href=\"http://www.chmaas.handshake.de/delphi/freeware/xvi32/xvi32.htm\" rel=\"noreferrer\">2</a> - I've found it to be fairly fast even for big  <a href=\"http://www.chmaas.handshake.de/delphi/freeware/xvi32/xvi32.htm\" rel=\"noreferrer\"></a> files.</p>\n"""
//    println(toJson(Map("body" -> convert_html_character(body))))
//        cleanData
    cleanQuestionData()
  }

  def dbCompact: Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
        .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
        .setTargetFileSizeBase(1024 * 1024 * 1024)
      val db = RocksDB.open(options, "post.db")
      db.compactRange()
      db.flush(new FlushOptions().setWaitForFlush(true))
      db.close()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def save_question_to_db(): Unit = {
    RocksDB.loadLibrary()
    val options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
    val db = RocksDB.open(options, "post.db")

    val write_options = new Options().setCreateIfMissing(true).setMaxWriteBufferNumber(3)
      .setCompressionType(CompressionType.LZ4_COMPRESSION).setWriteBufferSize(1024 * 1024 * 1024)
      .setTargetFileSizeBase(1024 * 1024 * 1024)
    val question_db = RocksDB.open(write_options, "question.db")

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
    println("end")

  }

  def cleanQuestionData(): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
      val question_db = RocksDB.open(options, "question.db")

      var count: Int = 0
      var ts: Long = System.currentTimeMillis()
      val all_lines: Int = 647
      var has_all: Int = 0
      var file_num = 1
      var json_len = 0
      val json_list = mutable.ListBuffer[String]()

      val has_ids =mapper.readValue(new File("all_ids.json"), classOf[mutable.HashSet[Int]])

      file_num = 2000
      val question_iterator = question_db.newIterator()
      question_iterator.seekToFirst()
      while (question_iterator.isValid) { //遍历所有问题
        val question = new String(question_iterator.value(), "utf-8")
        val question_map = jsonToMap(question)
        if (!has_ids.contains(question_map("Id").asInstanceOf[String].toInt)) {
          val question_body = convert_html_character(question_map("Body").asInstanceOf[String])
          val json_str = get_json(question_map + ("Body" -> question_body), Map.empty, question, "")
          json_list.append(json_str)
          json_len += json_str.getBytes("utf-8").size
          has_ids.add(question_map("Id").asInstanceOf[String].toInt)
          if (json_len >= 500 * 1024 * 1024) {
            val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
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
        val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
        json_list.foreach(s => pw.println(s))
        pw.flush()
        pw.close()
        json_list.clear()
        json_len = 0
        file_num += 1
        println(s"write new file $file_num")
      }

      question_iterator.close()
      question_db.close()
      println("end")

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def cleanData(): Unit = {
    try {
      RocksDB.loadLibrary()
      val options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)
      val db = RocksDB.open(options, "post.db")
      val question_db = RocksDB.open(options, "question.db")
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
      while (iterator.isValid) { //遍历所有答案
        val data = new String(iterator.value(), "utf-8")
        val map_data = jsonToMap(data)
        if (map_data("PostTypeId").asInstanceOf[String].toInt == 2) {
          count+=1
          val question = new String(question_db.get(map_data("ParentId").asInstanceOf[String].getBytes("utf-8")), "utf-8")
          val question_map = jsonToMap(question)
          val question_body = convert_html_character(question_map("Body").asInstanceOf[String])
          val body = convert_html_character(map_data("Body").asInstanceOf[String])
          val json_str = get_json(question_map + ("Body" -> question_body), map_data + ("Body" -> body), question, data)
          json_list.append(json_str)
          json_len += json_str.getBytes("utf-8").size
          has_ids.add(question_map("Id").asInstanceOf[String].toInt)
          if (json_len >= 500 * 1024 * 1024) {
            val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
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
        val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
        json_list.foreach(s => pw.println(s))
        pw.flush()
        pw.close()
        json_list.clear()
        json_len = 0
        file_num += 1
        println(s"write new file $file_num")
      }
      val pw_ids = new PrintWriter(s"all_ids.json", "utf-8")
      pw_ids.write(toJson(has_ids))
      pw_ids.flush()
      pw_ids.close()
      file_num=1000
      val question_iterator = question_db.newIterator()
      question_iterator.seekToFirst()
      while (question_iterator.isValid) { //遍历所有问题
        val question = new String(question_iterator.value(), "utf-8")
        println(new String(question_iterator.key()))
        println(question)
        val question_map = jsonToMap(question)
        if (!has_ids.contains(question_map("Id").asInstanceOf[String].toInt)) {
          val question_body = convert_html_character(question_map("Body").asInstanceOf[String])
          val json_str = get_json(question_map + ("Body" -> question_body), Map.empty, question, "")
          json_list.append(json_str)
          json_len += json_str.getBytes("utf-8").size
          has_ids.add(question_map("Id").asInstanceOf[String].toInt)
          if (json_len >= 500 * 1024 * 1024) {
            val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
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
        val pw = new PrintWriter(s"${file_num}.jsonl", "utf-8")
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
    val ts=System.currentTimeMillis()
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
        if (idx_end <=0){
          cut_pre_html(s"replace_pre_$num") = "<pre>"
          html_deal = html_deal.take(idx) + s"replace_pre_$num" + html_deal.drop(idx + 5)
        }else {
          val html_cut = html_deal.drop(idx).take(idx_end - idx + 6)
          cut_pre_html(s"replace_pre_$num") = html_cut
          html_deal = html_deal.take(idx) + s"replace_pre_$num" + html_deal.drop(idx_end + 6)
        }
        num += 1
        if (System.currentTimeMillis() - ts > 1000) {
          println("error")
        }
      }
      val cut_a_html=mutable.HashMap[String, String]()
      val html_sb=new StringBuilder(html_deal)
      while (html_sb.indexOf("<a ") != -1) { //提取非pre段的 a标签。如果没有body，则不处理
        val idx = html_sb.indexOf("<a ")
        val idx_over = html_sb.indexOf(">",idx)
        val idx_end = html_sb.indexOf("</a>", idx_over)
        if (idx_over == -1 || idx_end == -1){//非标准的a标签
          cut_a_html(s"replace_pre_$num")="<a "
          num += 1
          html_sb.delete(idx, idx + 3)
        }else{
          html_sb.delete(idx_end, idx_end + 4)
          html_sb.delete(idx, idx_over + 1)
        }
        if (System.currentTimeMillis()-ts >1000){
          println("error")
        }
      }
      //提取a链接的text
      cut_pre_html.foreach(kv => replaceAll(html_sb,kv._1, kv._2))
      cut_a_html.foreach(kv => replaceAll(html_sb,kv._1, kv._2))
      html = html_sb.toString()
    }
    html.replace("<pre>","```").replace("</pre>","```")
  }

  def  replaceAll (builder:StringBuilder ,from: String ,to: String ) {
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

  def get_json(question: Map[String, Any], data: Map[String, Any], question_str: String, data_str: String) = {
    toJson(Map(
      "id" -> question("Id"),
      "问" -> (question("Title").asInstanceOf[String] + "\n" + question("Body").asInstanceOf[String])
      , "答" -> data.getOrElse("Body", "")
      , "来源" -> "stackexchange"
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