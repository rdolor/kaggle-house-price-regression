package kaggle.houseprice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object HPRegression {

  private val trainSchema = StructType(
    Array(
      StructField("Id",LongType, false),
      StructField("MSSubClass",LongType, true),
      StructField("MSZoning",StringType, true),
      StructField("LotFrontage",LongType, true),
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
      StructField("GarageYrBlt",LongType, true),
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
      StructField("PoolQC",LongType, true),
      StructField("Fence",StringType, true),
      StructField("MiscFeature",LongType, true),
      StructField("MiscVal",LongType, true),
      StructField("MoSold",LongType, true),
      StructField("YrSold",LongType, true),
      StructField("SaleType",StringType, true),
      StructField("SaleCondition",StringType, true),
      StructField("SalePrice",LongType, false)
    )
  )

  private val testSchema = StructType(
    Array(
      StructField("Id",LongType, false),
      StructField("MSSubClass",LongType, true),
      StructField("MSZoning",StringType, true),
      StructField("LotFrontage",LongType, true),
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
      StructField("GarageYrBlt",LongType, true),
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
      StructField("PoolQC",LongType, true),
      StructField("Fence",StringType, true),
      StructField("MiscFeature",LongType, true),
      StructField("MiscVal",LongType, true),
      StructField("MoSold",LongType, true),
      StructField("YrSold",LongType, true),
      StructField("SaleType",StringType, true),
      StructField("SaleCondition",StringType, true)
    )
  )

  def main(args: Array[String]):Unit={

    val spark = SparkSession.builder
      .appName("Kaggle-House-Price-Regression")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val trainDataDir = args(0)
    val testDataDir = args(1)
    val outputDataDir = args(2)
    val trainData =
      loadData(spark = spark, fileDir = trainDataDir, scheme = trainSchema)
    val testData =
      loadData(spark = spark, fileDir = testDataDir, scheme = testSchema)

  }

  def loadData(
                spark: SparkSession,
                fileDir: String,
                scheme: StructType
              ): (DataFrame) = {

    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(scheme)
      .load(fileDir)

    df
  }

}
