import io.kotlintest.inspectors.forAll
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.matchers.numerics.shouldBeGreaterThan
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec


class IntegrationTests : StringSpec() {
    val datasources = listOf(
        "sqlite" to ::getSqliteDatasource,
        "mssql" to ::getMssqlDatasource
    )

    init {
        "rules designed to fail should fail" {
            datasources.forAll { (dialectName, dsFactory) ->
                val suite = testSuite {
                    val ds = datasource(name = "dw", dialect = dialectName) {
                        table("customer") {
                            rows {
                                shouldBeAtLeast(1000)
                                shouldBeAtMost(1)
                                shouldBeBetween(1000, 3000)
                                shouldEqual(4000)
                            }
                            fields {
                                intField("id")
                                textField("name", caseSensitive = false) {
                                    shouldEndWith("Z")
                                    shouldBeLike("ar")
                                    shouldBeOneOf("Mark", "Steve")
                                    shouldStartWith("M")
                                }
                                dateTimeField("date_added") {
                                    shouldBeAfter("2099-01-01")
                                    shouldBeBefore("1999-12-31")
                                    shouldBeBetween("1999-01-01", "1999-12-31")
                                    shouldBeOnOrAfter("2099-12-31")
                                    shouldBeOnOrBefore("1999-01-01")
                                }
                            }
                        }
                        table("item") {
                            fields {
                                intField("id") {
                                    shouldBeAtLeast(999_999)
                                    shouldBeAtMost(-999_999)
                                    shouldBeBetween(-99, 0)
                                    shouldBeOneOf(724, 421, 999)
                                }
                                decimalField("price") {
                                    shouldBeAtLeast(9999)
                                    shouldBeAtMost(-9999)
                                    shouldBeBetween(9999, 99999)
                                    shouldBeOneOf(12345, 6789, 4321)
                                }
                                floatField("weight") {
                                    shouldBeAtLeast(999.9999)
                                    shouldBeAtMost(-999.99)
                                    shouldBeBetween(-99.9, -1.32)
                                    shouldBeOneOf(-13.2, -24.1, 997.8)
                                }
                            }
                        }
                        table("sale") {
                            fields {
                                intField("id")
                                dateField("sales_date")
                                intField("customer_id")
                                intField("item_id")
                                intField("quantity_sold")
                            }
                        }
                    }

                    tablesShould {
                        haveMatchingRows(ds["customer"], ds["item"])
                        haveMatchingTotals(ds["customer"].intField("id"), ds["item"].intField("id"))
                    }
                }

                val ds = dsFactory()
                withTestDb(ds = ds) {
                    exec(
                        """
                        INSERT INTO customer (id, name, date_added) 
                        VALUES 
                            (1, 'Mark', '2020-01-02'),
                            (2, 'Steve', '2020-02-01T03:12:02.321'),
                            (3, 'Mary', '2020-03-01T04:22:01.333'),
                            (4, 'Bill', '2020-04-01T04:22:01.333')
                    """
                    )
                    exec(
                        """
                        INSERT INTO item (id, name, weight, price)
                        VALUES 
                            (1, 'Fork', 12.3, 2.27),
                            (2, 'Fork', 12.3, 2.27),
                            (3, 'Spork', 14.2, 3.32),
                            (4, 'Plate', 700.2, 12.13),
                            (5, 'Bowl', 600.27, 8.24)
                    """
                    )
                    exec(
                        """
                        INSERT INTO sale (id, sales_date, customer_id, item_id, quantity_sold)
                        VALUES 
                            (1, '2020-02-01T02:05:11.123', 1, 1, 12),
                            (2, '2020-02-01T02:04:13.223', 3, 1, 7),
                            (3, '2020-02-02T02:04:01.213', 2, 1, 4),
                            (4, '2020-03-02T02:04:01.213', 3, 1, 23)
                    """
                    )
                    commit()
                    val actual = suite.runTests(mapOf("dw" to ds))
                    actual.forAll {
                        it should beInstanceOf<TestResult.Failed>()
                    }
                    actual.count() shouldBeGreaterThan 0
                    actual.count() shouldBe suite.rules.count()
                }
            }
        }

        "rules designed to pass should pass" {
            datasources.forAll { (dialectName, dsFactory) ->
                val suite = testSuite {
                    val ds = datasource(name = "dw", dialect = dialectName) {
                        table("customer") {
                            rows {
                                shouldBeAtLeast(1)
                                shouldBeAtMost(100)
                                shouldBeBetween(1, 100)
                                shouldEqual(4)
                            }
                            fields {
                                intField("id")

                                dateTimeField("date_added") {
                                    shouldBeAfter("2000-01-01")
                                    shouldBeBefore("2099-12-31")
                                    shouldBeBetween("2001-01-01", "2020-12-31")
                                    shouldBeOnOrAfter("2000-01-01")
                                    shouldBeOnOrBefore("2099-12-31")
                                }
                            }
                        }
                        table("item") {
                            fields {
                                intField("id") {
                                    shouldBeAtLeast(1)
                                    shouldBeAtMost(9999)
                                    shouldBeBetween(0, 9999)
                                    shouldBeOneOf(1, 3)
                                }
                                textField("name", caseSensitive = false) {
                                    shouldEndWith("ula")
                                    shouldBeLike("ula")
                                    shouldBeOneOf("Sporkula", "Spatula")
                                    shouldStartWith("S")
                                }
                                decimalField("price") {
                                    shouldBeAtLeast(0)
                                    shouldBeAtMost(999.99)
                                    shouldBeBetween(0, 99999)
                                    shouldBeOneOf(2.27, 3.32)
                                }
                                floatField("weight") {
                                    shouldBeAtLeast(0)
                                    shouldBeAtMost(999.99)
                                    shouldBeBetween(0, 9999)
                                    shouldBeOneOf(12.3, 14.2)
                                }
                            }
                        }
                        table("sale") {
                            fields {
                                intField("id")
                                dateField("sales_date")
                                intField("customer_id")
                                intField("item_id")
                                intField("quantity_sold")
                            }
                        }
                    }

                    tablesShould {
                        haveMatchingRows(ds["customer"], ds["sale"])
                        haveMatchingTotals(ds["customer"].intField("id"), ds["sale"].intField("id"))
                    }
                }

                val ds = dsFactory()
                withTestDb(ds = ds) {
                    exec(
                        """
                        INSERT INTO customer (id, name, date_added) 
                        VALUES 
                            (1, 'Mark', '2020-01-02'),
                            (2, 'Steve', '2020-02-01T03:12:02.321'),
                            (3, 'Mary', '2020-03-01T04:22:01.333'),
                            (4, 'Bill', '2020-04-01T04:22:01.333')
                    """
                    )
                    exec(
                        """
                        INSERT INTO item (id, name, weight, price)
                        VALUES 
                            (1, 'Spatula', 12.3, 2.27),
                            (3, 'Sporkula', 14.2, 3.32)
                    """
                    )
                    exec(
                        """
                        INSERT INTO sale (id, sales_date, customer_id, item_id, quantity_sold)
                        VALUES 
                            (1, '2020-02-01T02:05:11.123', 1, 1, 12),
                            (2, '2020-02-01T02:04:13.223', 3, 1, 7),
                            (3, '2020-02-02T02:04:01.213', 2, 1, 4),
                            (4, '2020-03-02T02:04:01.213', 3, 1, 23)
                    """
                    )
                    commit()
                    val actual = suite.runTests(mapOf("dw" to ds))
                    actual.forAll {
                        it should beInstanceOf<TestResult.Passed>()
                    }
                    actual.count() shouldBeGreaterThan 0
                    actual.count() shouldBe suite.rules.count()
                }
            }
        }
    }
}
