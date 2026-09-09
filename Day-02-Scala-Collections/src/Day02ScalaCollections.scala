object Day02ScalaCollections {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // 1. Sales List - map, filter, flatMap, reduce
    // -----------------------------------------

    val sales = List(100, 250, 150, 300, 200)

    println("Original Sales:")
    println(sales)

    val increasedSales = sales.map(amount => amount + 50)

    println("\nSales after adding 50:")
    println(increasedSales)

    val highSales = sales.filter(amount => amount >= 200)

    println("\nSales >= 200:")
    println(highSales)

    val nestedSales = List(
      List(100, 200),
      List(300, 400),
      List(500)
    )

    val flatSales = nestedSales.flatMap(sale => sale)

    println("\nFlattened Sales:")
    println(flatSales)

    val totalSales = sales.reduce((a, b) => a + b)

    println("\nTotal Sales:")
    println(totalSales)


    // -----------------------------------------
    // 2. Vector - Indexed Customer Records
    // -----------------------------------------

    val customers = Vector(
      "Ravi",
      "Priya",
      "Vignesh",
      "Teju",
      "Anil"
    )

    println("\nCustomer Records:")
    println(customers)

    println("\nCustomer at index 2:")
    println(customers(2))


    // -----------------------------------------
    // 3. Map - Product Quantities and Prices
    // -----------------------------------------

    val productQuantities = Map(
      "Laptop" -> 2,
      "Mobile" -> 5,
      "Headphones" -> 10,
      "Keyboard" -> 4
    )

    val productPrices = Map(
      "Laptop" -> 60000,
      "Mobile" -> 25000,
      "Headphones" -> 2000,
      "Keyboard" -> 1500
    )

    println("\nProduct Quantities:")
    println(productQuantities)

    println("\nProduct Prices:")
    println(productPrices)

    val productRevenue = productQuantities.map {
      case (product, quantity) =>
        val price = productPrices(product)
        product -> (quantity * price)
    }

    println("\nProduct Revenue:")
    println(productRevenue)


    // -----------------------------------------
    // 4. For-Comprehension
    // -----------------------------------------

    val customerOrders = List(
      ("Ravi", "Laptop"),
      ("Priya", "Mobile"),
      ("Vignesh", "Headphones")
    )

    val orderSummary = for {
      (customer, product) <- customerOrders
      price <- productPrices.get(product)
    } yield (customer, product, price)

    println("\nCustomer Order Summary:")
    orderSummary.foreach(println)


    // -----------------------------------------
    // 5. Daily Sales Summary
    // -----------------------------------------

    val dailySales = List(
      ("Laptop", 2, 60000),
      ("Mobile", 5, 25000),
      ("Headphones", 10, 2000),
      ("Keyboard", 4, 1500)
    )

    val dailyRevenue = dailySales.map {
      case (product, quantity, price) =>
        quantity * price
    }.sum

    println("\nDaily Sales Summary:")
    println(s"Total Revenue: ₹$dailyRevenue")

    val totalItems = dailySales.map {
      case (_, quantity, _) => quantity
    }.sum

    println(s"Total Items Sold: $totalItems")
  }
}
