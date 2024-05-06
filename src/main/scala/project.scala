import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.{BufferedSource, Source}
import scala.math.BigDecimal.RoundingMode
import java.sql.{Connection, DriverManager}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.config.Configurator

object project extends App {

  // Define a case class to represent an order
  case class Order(timestamp: LocalDate, pCategory: String, expDate: LocalDate,
                   quantity: Double, uPrice: Double, channel: String, pMethod: String,
                   price_before: Double, discount: Double, price_after: Double)

  // Initialize log4j configuration
  Configurator.initialize(null,"E:\\DM44-Alex\\20- Scala\\Project\\project\\src\\main\\Resources\\log4j2.xml")
  val logger = LogManager.getLogger(getClass.getName)

  // Database connection details and Establish connection
  val url = "jdbc:mysql://localhost:3306/Orders"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "1234"
  val connection: Connection = DriverManager.getConnection(url, username, password)

  // Function to split a line of input and create an Order object
  def split_line(line: String): Order = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val order = line.split(",")
    val timeStamp = order(0).substring(0, order(0).indexOf('T'))
    val date = LocalDate.parse(timeStamp, dateFormat)
    val pCategory = order(1).split('-')(0).trim
    val expDate = LocalDate.parse(order(2), dateFormat)
    val quantity = order(3).toDouble
    val uPrice = order(4).toDouble
    val channel = order(5)
    val method = order(6)
    Order(date, pCategory, expDate, quantity, uPrice, channel, method, 0.0, 0.0, 0.0)
  }

  // Function to check if the date is March 23rd
  def march23Qualify(d: LocalDate): Boolean = d.getMonthValue == 3 && d.getDayOfMonth == 23

  // Function to calculate discount based on March 23rd qualification
  def march23Calc(boolean: Boolean): Double = if (boolean) 0.50 else 0.0

  // Function to calculate discount based on quantity
  def quantityCalc(q: Double): Double = q match {
    case x if x >= 6 && x <= 9 => 0.05
    case x if x >= 10 && x <= 14 => 0.07
    case x if x >= 15 => 0.10
    case _ => 0
  }

  // Function to calculate discount based on product category
  def categoryCalc(cat: String): Double = cat match {
    case "Cheese" => 0.1
    case "Wine" => 0.05
    case _ => 0
  }

  // Function to check if the product expires in less than 30 days
  def expireQualify(t_d:LocalDate,ex_d:LocalDate):Int ={
    val diff = java.time.temporal.ChronoUnit.DAYS.between(t_d, ex_d).toInt
    diff match {
      case x if x < 30 => diff
      case _ => 0
    }
  }

  // Function to calculate discount based on product expiration date
  def expireCalc(diff:Int):Double ={
    if (diff == 0) 0.0
    else{
      val days = 29 - diff
      if (days < 0) 0.0
      else BigDecimal(0.01 +(days * 0.01)).setScale(2, RoundingMode.HALF_UP).toDouble
    }
  }

  // Function to check if the sale is made through the App
  def appQualify(ch :String):Boolean = ch match{case "App" => true case _ => false}

  // Function to calculate discount based on App qualification
  def appCalc(bol: Boolean,quantity:Double):Double = {
    if (bol) BigDecimal((math.ceil(quantity/5))*0.05).setScale(2, RoundingMode.HALF_UP).toDouble
    else 0.0
  }

  // Function to check if the sale is made using a Visa card
  def visaQualify(method:String):Boolean = method match { case "Visa" => true  case _ => false}

  // Function to calculate discount based on Visa qualification
  def visaCalc(bool: Boolean): Double = if (bool) 0.05 else 0.0

  try {

    // Create table if not exists
    val createTableQuery =
      """
        |CREATE TABLE IF NOT EXISTS orders (
        |  id INT AUTO_INCREMENT PRIMARY KEY,
        |  timestamp DATE,
        |  product_name VARCHAR(255),
        |  expiry_date DATE,
        |  quantity DOUBLE,
        |  unit_price DOUBLE,
        |  channel VARCHAR(255),
        |  payment_method VARCHAR(255),
        |  price_before DOUBLE,
        |  discount DOUBLE,
        |  price_after DOUBLE
        |);
        |""".stripMargin

    val statement = connection.createStatement()
    statement.execute(createTableQuery)
    logger.info(createTableQuery)

    // Read data from file and process orders
    val source: BufferedSource = Source.fromFile("E:\\DM44-Alex\\20- Scala\\Project\\project\\src\\main\\Resources\\TRX1000.csv")
    val lines = source.getLines().drop(1).toList // drop header

    val orders = lines.map(split_line)

    // Calculate discounts for each order
    val discounts = orders.map { order =>
      List(
        march23Calc(march23Qualify(order.timestamp)),
        quantityCalc(order.quantity),
        categoryCalc(order.pCategory),
        expireCalc(expireQualify(order.timestamp, order.expDate)),
        appCalc(appQualify(order.channel),order.quantity),
        visaCalc(visaQualify(order.pMethod))
      ).filter(_ > 0).sorted.reverse
    }

    // Calculate average discounts for each order
    val averageDiscounts = discounts.map { discount =>
      if (discount.length >= 2) {
        BigDecimal(discount.take(2).sum / 2.0).setScale(2, RoundingMode.HALF_UP).toDouble
      } else {
        discount.headOption.getOrElse(0.0)
      }
    }

    // Calculate prices before discounts
    val pricesBefore = orders.map(order => BigDecimal(order.quantity * order.uPrice).setScale(2,RoundingMode.HALF_UP).toDouble)

    // Calculate prices after discounts
    val pricesAfter = pricesBefore.zip(averageDiscounts).map { case (priceBefore, averageDiscount) =>
      val price_after = priceBefore - (priceBefore * averageDiscount)
      BigDecimal(price_after).setScale(2, RoundingMode.HALF_UP).toDouble
    }

    // Insert orders into the database
    orders.zip(averageDiscounts).zip(pricesBefore).zip(pricesAfter).foreach { case (((order, averageDiscount), priceBefore), priceAfter) =>

      val insertQuery =
        s"""
           |INSERT INTO orders (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, price_before, discount, price_after)
           |VALUES ('${order.timestamp}', '${order.pCategory}', '${order.expDate}', ${order.quantity}, ${order.uPrice}, '${order.channel}', '${order.pMethod}', ${priceBefore}, ${averageDiscount}, ${priceAfter});
           |""".stripMargin
      statement.executeUpdate(insertQuery)
      logger.info(s" $insertQuery")

    }
    logger.info("################## Successfully Processed ##################")
  }catch {
    case e: Exception => e.printStackTrace()
  } finally {
    connection.close()
  }

}
