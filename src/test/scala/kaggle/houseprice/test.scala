package kaggle.houseprice

import kaggle.houseprice.HPRegression

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class TestRegression extends FunSuite {

  val spark = SparkSession.builder
    .appName("Test-Kaggle-House-Price-Regression")
    .master("local[*]")
    .getOrCreate()

  val trainSchema = StructType(
    Array(
      StructField("Id",LongType, false),
      StructField("MSSubClass",LongType, true),
      StructField("MSZoning",StringType, true),
      StructField("LotFrontage",StringType, true),//,LongType, true),
      StructField("LotArea",LongType, true),
      StructField("Street",StringType, true),
      StructField("Alley",StringType, true),
      StructField("LotShape",StringType, true),
      StructField("LandContour",StringType, true),
      StructField("Utilities",StringType, true),
      StructField("LotConfig",StringType, true),
      StructField("LandSlope",StringType, true),
      StructField("Neighborhood",StringType, true),
      StructField("Condition1",StringType, true),
      StructField("Condition2",StringType, true),
      StructField("BldgType",StringType, true),
      StructField("HouseStyle",StringType, true),
      StructField("OverallQual",LongType, true),
      StructField("OverallCond",LongType, true),
      StructField("YearBuilt",LongType, true),
      StructField("YearRemodAdd",LongType, true),
      StructField("RoofStyle",StringType, true),
      StructField("RoofMatl",StringType, true),
      StructField("Exterior1st",StringType, true),
      StructField("Exterior2nd",StringType, true),
      StructField("MasVnrType",StringType, true),
      StructField("MasVnrArea",LongType, true),
      StructField("ExterQual",StringType, true),
      StructField("ExterCond",StringType, true),
      StructField("Foundation",StringType, true),
      StructField("BsmtQual",StringType, true),
      StructField("BsmtCond",StringType, true),
      StructField("BsmtExposure",StringType, true),
      StructField("BsmtFinType1",StringType, true),
      StructField("BsmtFinSF1",LongType, true),
      StructField("BsmtFinType2",StringType, true),
      StructField("BsmtFinSF2",LongType, true),
      StructField("BsmtUnfSF",LongType, true),
      StructField("TotalBsmtSF",LongType, true),
      StructField("Heating",StringType, true),
      StructField("HeatingQC",StringType, true),
      StructField("CentralAir",StringType, true),
      StructField("Electrical",StringType, true),
      StructField("1stFlrSF",LongType, true),
      StructField("2ndFlrSF",LongType, true),
      StructField("LowQualFinSF",LongType, true),
      StructField("GrLivArea",LongType, true),
      StructField("BsmtFullBath",LongType, true),
      StructField("BsmtHalfBath",LongType, true),
      StructField("FullBath",LongType, true),
      StructField("HalfBath",LongType, true),
      StructField("BedroomAbvGr",LongType, true),
      StructField("KitchenAbvGr",LongType, true),
      StructField("KitchenQual",StringType, true),
      StructField("TotRmsAbvGrd",LongType, true),
      StructField("Functional",StringType, true),
      StructField("Fireplaces",LongType, true),
      StructField("FireplaceQu",StringType, true),
      StructField("GarageType",StringType, true),
      StructField("GarageYrBlt",StringType, true),//,LongType, true),
      StructField("GarageFinish",StringType, true),
      StructField("GarageCars",LongType, true),
      StructField("GarageArea",LongType, true),
      StructField("GarageQual",StringType, true),
      StructField("GarageCond",StringType, true),
      StructField("PavedDrive",StringType, true),
      StructField("WoodDeckSF",LongType, true),
      StructField("OpenPorchSF",LongType, true),
      StructField("EnclosedPorch",LongType, true),
      StructField("3SsnPorch",LongType, true),
      StructField("ScreenPorch",LongType, true),
      StructField("PoolArea",LongType, true),
      StructField("PoolQC",StringType, true),//,LongType, true),
      StructField("Fence",StringType, true),
      StructField("MiscFeature",StringType, true),//,LongType, true),
      StructField("MiscVal",LongType, true),
      StructField("MoSold",LongType, true),
      StructField("YrSold",LongType, true),
      StructField("SaleType",StringType, true),
      StructField("SaleCondition",StringType, true),
      StructField("SalePrice",LongType, false)
    )
  )
  val testSchema = StructType(
    Array(
      StructField("Id",LongType, false),
      StructField("MSSubClass",LongType, true),
      StructField("MSZoning",StringType, true),
      StructField("LotFrontage",StringType, true),//,LongType, true),
      StructField("LotArea",LongType, true),
      StructField("Street",StringType, true),
      StructField("Alley",StringType, true),
      StructField("LotShape",StringType, true),
      StructField("LandContour",StringType, true),
      StructField("Utilities",StringType, true),
      StructField("LotConfig",StringType, true),
      StructField("LandSlope",StringType, true),
      StructField("Neighborhood",StringType, true),
      StructField("Condition1",StringType, true),
      StructField("Condition2",StringType, true),
      StructField("BldgType",StringType, true),
      StructField("HouseStyle",StringType, true),
      StructField("OverallQual",LongType, true),
      StructField("OverallCond",LongType, true),
      StructField("YearBuilt",LongType, true),
      StructField("YearRemodAdd",LongType, true),
      StructField("RoofStyle",StringType, true),
      StructField("RoofMatl",StringType, true),
      StructField("Exterior1st",StringType, true),
      StructField("Exterior2nd",StringType, true),
      StructField("MasVnrType",StringType, true),
      StructField("MasVnrArea",LongType, true),
      StructField("ExterQual",StringType, true),
      StructField("ExterCond",StringType, true),
      StructField("Foundation",StringType, true),
      StructField("BsmtQual",StringType, true),
      StructField("BsmtCond",StringType, true),
      StructField("BsmtExposure",StringType, true),
      StructField("BsmtFinType1",StringType, true),
      StructField("BsmtFinSF1",LongType, true),
      StructField("BsmtFinType2",StringType, true),
      StructField("BsmtFinSF2",LongType, true),
      StructField("BsmtUnfSF",LongType, true),
      StructField("TotalBsmtSF",LongType, true),
      StructField("Heating",StringType, true),
      StructField("HeatingQC",StringType, true),
      StructField("CentralAir",StringType, true),
      StructField("Electrical",StringType, true),
      StructField("1stFlrSF",LongType, true),
      StructField("2ndFlrSF",LongType, true),
      StructField("LowQualFinSF",LongType, true),
      StructField("GrLivArea",LongType, true),
      StructField("BsmtFullBath",LongType, true),
      StructField("BsmtHalfBath",LongType, true),
      StructField("FullBath",LongType, true),
      StructField("HalfBath",LongType, true),
      StructField("BedroomAbvGr",LongType, true),
      StructField("KitchenAbvGr",LongType, true),
      StructField("KitchenQual",StringType, true),
      StructField("TotRmsAbvGrd",LongType, true),
      StructField("Functional",StringType, true),
      StructField("Fireplaces",LongType, true),
      StructField("FireplaceQu",StringType, true),
      StructField("GarageType",StringType, true),
      StructField("GarageYrBlt",StringType, true),//,LongType, true),
      StructField("GarageFinish",StringType, true),
      StructField("GarageCars",LongType, true),
      StructField("GarageArea",LongType, true),
      StructField("GarageQual",StringType, true),
      StructField("GarageCond",StringType, true),
      StructField("PavedDrive",StringType, true),
      StructField("WoodDeckSF",LongType, true),
      StructField("OpenPorchSF",LongType, true),
      StructField("EnclosedPorch",LongType, true),
      StructField("3SsnPorch",LongType, true),
      StructField("ScreenPorch",LongType, true),
      StructField("PoolArea",LongType, true),
      StructField("PoolQC",StringType, true),//,LongType, true),
      StructField("Fence",StringType, true),
      StructField("MiscFeature",StringType, true),//,LongType, true),
      StructField("MiscVal",LongType, true),
      StructField("MoSold",LongType, true),
      StructField("YrSold",LongType, true),
      StructField("SaleType",StringType, true),
      StructField("SaleCondition",StringType, true)
    )
  )
  test("Test load data") {
    val spark = SparkSession.builder
      .appName("Test-House-Price-Regression")
      .master("local[*]")
      .getOrCreate()
    val trainData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/train.csv",
      scheme = trainSchema
    )
    val testData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/test.csv",
      scheme = testSchema
    )

    trainData.printSchema()

    assert(trainData.count() == 1460)
    assert(testData.count() == 1459)

    //trainData.show()

    spark.stop()
  }

  test("Test parseData") {
    val spark = SparkSession.builder
      .appName("Test-House-Price-Regression")
      .master("local[*]")
      .getOrCreate()
    val trainData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/train.csv",
      scheme = trainSchema
    )
    val testData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/test.csv",
      scheme = testSchema
    )

    val (parsedTrainData, parsedTestData) = HPRegression.parseData(
      trainData = trainData,
      testData = testData
    )
    //parsedTrainData.show()
    //parsedTestData.show()

    val trainColumns = parsedTrainData.columns.toSeq
    val testColumns = parsedTestData.columns.toSeq
    assert(
      trainColumns == Array(
        "label",
        "features"
      ).toSeq
    )
    assert(
      testColumns == Array(
        "features"
      ).toSeq
    )

    assert(parsedTrainData.count() == 1460)
    assert(parsedTestData.count() == 1459)

    assert(parsedTrainData.columns.size == 2)
    assert(parsedTestData.columns.size == 1)

    //assert(parsedTrainData.head(5).isEmpty)
    //assert(parsedTrainData.take(1).isEmpty)
    //assert(parsedTestData.head(5).isEmpty)
    //assert(parsedTestData.take(1).isEmpty)

    //assert(parsedTestData.isEmpty)
    //assert(parsedTrainData.isEmpty)

    spark.stop()
  }

  test("Test train") {
    // Load training data
    val spark = SparkSession.builder
      .appName("Test-House-Price-Regression")
      .master("local[*]")
      .getOrCreate()
    val trainData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/train.csv",
      scheme = trainSchema
    )
    val testData = HPRegression.loadData(
      spark = spark,
      fileDir = "data/test.csv",
      scheme = testSchema
    )

    val (parsedTrainData, parsedTestData) = HPRegression.parseData(
      trainData = trainData,
      testData = testData
    )

    val prediction = HPRegression.trainAndPredict(
      trainData = parsedTrainData,
      testData = parsedTestData
    )
    prediction.show()

    assert(prediction.count() == 1459)
    spark.stop()
  }


//  test("Test write to csv") {
//    // Load training data
//    val spark = SparkSession.builder
//      .appName("Test-House-Price-Regression")
//      .master("local[*]")
//      .getOrCreate()
//    val trainData = HPRegression.loadData(
//      spark = spark,
//      fileDir = "data/train.csv",
//      scheme = trainSchema
//    )
//    val testData = HPRegression.loadData(
//      spark = spark,
//      fileDir = "data/test.csv",
//      scheme = testSchema
//    )
//
//    val (parsedTrainData, parsedTestData) = HPRegression.parseData(
//      trainData = trainData,
//      testData = testData
//    )
//
//    val prediction = HPRegression.trainAndPredict(
//      trainData = parsedTrainData,
//      testData = parsedTestData
//    )
//
//    val res = HPRegression.write2CSV(
//      prediction = prediction,
//      testData = testData,
//      outputDir = "/tmp/submit",
//      isWrite = false
//    )
//    res.show()
//
//    assert(res.count() == 1459)
//    spark.stop()
//  }

}
