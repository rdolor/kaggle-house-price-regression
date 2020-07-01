package kaggle.houseprice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression,RandomForestRegressor}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator


object HPRegression {

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

    val (parsedTrainData, parsedTestData) = parseData(
      trainData = trainData,
      testData = testData
    )

    val prediction = trainAndPredict(
      trainData = parsedTrainData,
      testData = parsedTestData
    )

//    val res = write2CSV(
//      prediction = prediction,
//      testData = testData,
//      outputDir = outputDataDir,
//      isWrite = true
//    )

  }

  def loadData(spark: SparkSession,
                fileDir: String,
                scheme: StructType
              ): (DataFrame) = {

    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(scheme)
      //.option("inferSchema","true")
      .load(fileDir)

    df
  }

  def parseData(
                 trainData: DataFrame,
                 testData: DataFrame
               ): (DataFrame, DataFrame) = {
    var parsedTrainData = trainData
    var parsedTestData = testData
    val dropFeatures = List("Id","GarageYrBlt","1stFlrSF","TotRmsAbvGrd","GarageArea", "PoolArea","MoSold","3SsnPorch","BsmtFinSF2","BsmtHalfBath", "MiscVal","LowQualFinSF","YrSold","OverallCond","MSSubClass", "Alley","PoolQC","Fence","MiscFeature","FireplaceQu"
      //"MasVnrArea", "OverallQual", "LotArea", "YearRemodAdd", "BsmtFinSF1", "WoodDeckSF", "GrLivArea", "KitchenAbvGr", "TotalBsmtSF", "BedroomAbvGr", "YearBuilt", "GarageCars", "2ndFlrSF", "BsmtUnfSF", "ScreenPorch", "HalfBath", "EnclosedPorch", "OpenPorchSF", "BsmtFullBath", "FullBath", "Fireplaces",
      //"GarageCond", "RoofStyle","BsmtQual", "Heating", "Functional", "SaleType", "Condition1", "RoofMatl", "KitchenQual", "GarageType", "BsmtCond", "ExterQual", "LotShape", "Condition2", "Neighborhood", "ExterCond", "CentralAir", "MSZoning","LotFrontage", "LotConfig", "GarageFinish", "PavedDrive", "Exterior1st", "Street", "Electrical", "HeatingQC", "SaleCondition", "Foundation", "LandSlope", "BsmtExposure", "GarageQual", "BldgType", "BsmtFinType1", "Exterior2nd", "Utilities", "LandContour", "BsmtFinType2", "HouseStyle", "MasVnrType"
      )
    val oneHotEncodeFeatures = Array("GarageCond", "RoofStyle","BsmtQual", "Heating", "Functional", "SaleType", "Condition1", "RoofMatl", "KitchenQual", "GarageType", "BsmtCond", "ExterQual", "LotShape", "Condition2", "Neighborhood", "ExterCond", "CentralAir", "MSZoning","LotFrontage", "LotConfig", "GarageFinish", "PavedDrive", "Exterior1st", "Street", "Electrical", "HeatingQC", "SaleCondition", "Foundation", "LandSlope", "BsmtExposure", "GarageQual", "BldgType", "BsmtFinType1", "Exterior2nd", "Utilities", "LandContour", "BsmtFinType2", "HouseStyle", "MasVnrType")
    val featuresName = Array(
      //"OverallQual",
      "MasVnrArea", "OverallQual", "LotArea", "YearRemodAdd", "BsmtFinSF1", "WoodDeckSF", "GrLivArea", "KitchenAbvGr", "TotalBsmtSF", "BedroomAbvGr", "YearBuilt", "GarageCars", "2ndFlrSF", "BsmtUnfSF", "ScreenPorch", "HalfBath", "EnclosedPorch", "OpenPorchSF", "BsmtFullBath", "FullBath", "Fireplaces",
      "GarageCond_vec", "RoofStyle_vec", "BsmtQual_vec", "Heating_vec", "Functional_vec", "SaleType_vec", "Condition1_vec", "RoofMatl_vec", "KitchenQual_vec", "GarageType_vec", "BsmtCond_vec", "ExterQual_vec", "LotShape_vec", "Condition2_vec", "Neighborhood_vec", "ExterCond_vec", "CentralAir_vec", "MSZoning_vec","LotFrontage_vec", "LotConfig_vec", "GarageFinish_vec", "PavedDrive_vec", "Exterior1st_vec", "Street_vec", "Electrical_vec", "HeatingQC_vec", "SaleCondition_vec", "Foundation_vec", "LandSlope_vec", "BsmtExposure_vec", "GarageQual_vec", "BldgType_vec", "BsmtFinType1_vec", "Exterior2nd_vec", "Utilities_vec", "LandContour_vec", "BsmtFinType2_vec", "HouseStyle_vec", "MasVnrType_vec"
    )

    val averageMasVnrArea= parsedTrainData.select(mean(parsedTrainData("MasVnrArea"))).first().getDouble(0)
    val averageOverallQual= parsedTrainData.select(mean(parsedTrainData("OverallQual"))).first().getDouble(0)
    val averageLotArea= parsedTrainData.select(mean(parsedTrainData("LotArea"))).first().getDouble(0)
    val averageYearRemodAdd= parsedTrainData.select(mean(parsedTrainData("YearRemodAdd"))).first().getDouble(0)
    val averageBsmtFinSF1= parsedTrainData.select(mean(parsedTrainData("BsmtFinSF1"))).first().getDouble(0)
    val averageWoodDeckSF= parsedTrainData.select(mean(parsedTrainData("WoodDeckSF"))).first().getDouble(0)
    val averageGrLivArea= parsedTrainData.select(mean(parsedTrainData("GrLivArea"))).first().getDouble(0)
    val averageKitchenAbvGr= parsedTrainData.select(mean(parsedTrainData("KitchenAbvGr"))).first().getDouble(0)
    val averageTotalBsmtSF= parsedTrainData.select(mean(parsedTrainData("TotalBsmtSF"))).first().getDouble(0)
    val averageBedroomAbvGr= parsedTrainData.select(mean(parsedTrainData("BedroomAbvGr"))).first().getDouble(0)
    val averageYearBuilt= parsedTrainData.select(mean(parsedTrainData("YearBuilt"))).first().getDouble(0)
    val averageGarageCars= parsedTrainData.select(mean(parsedTrainData("GarageCars"))).first().getDouble(0)
    val average2ndFlrSF= parsedTrainData.select(mean(parsedTrainData("2ndFlrSF"))).first().getDouble(0)
    val averageBsmtUnfSF= parsedTrainData.select(mean(parsedTrainData("BsmtUnfSF"))).first().getDouble(0)
    val averageScreenPorch= parsedTrainData.select(mean(parsedTrainData("ScreenPorch"))).first().getDouble(0)
    val averageHalfBath= parsedTrainData.select(mean(parsedTrainData("HalfBath"))).first().getDouble(0)
    val averageEnclosedPorch= parsedTrainData.select(mean(parsedTrainData("EnclosedPorch"))).first().getDouble(0)
    val averageOpenPorchSF= parsedTrainData.select(mean(parsedTrainData("OpenPorchSF"))).first().getDouble(0)
    val averageBsmtFullBath= parsedTrainData.select(mean(parsedTrainData("BsmtFullBath"))).first().getDouble(0)
    val averageFullBath= parsedTrainData.select(mean(parsedTrainData("FullBath"))).first().getDouble(0)
    val averageFireplaces= parsedTrainData.select(mean(parsedTrainData("Fireplaces"))).first().getDouble(0)

    // drop unuse columns
    for (feature <- dropFeatures) {
      parsedTrainData = parsedTrainData.drop(feature)
      parsedTestData = parsedTestData.drop(feature)
    }
    // fill null value
    parsedTrainData = parsedTrainData.na.fill(Map(
      "MasVnrArea" -> averageMasVnrArea,
      "OverallQual" -> averageOverallQual,
      "LotArea" -> averageLotArea,
      "YearRemodAdd" -> averageYearRemodAdd,
      "BsmtFinSF1" -> averageBsmtFinSF1,
      "WoodDeckSF" -> averageWoodDeckSF,
      "GrLivArea" -> averageGrLivArea,
      "KitchenAbvGr" -> averageKitchenAbvGr,
      "TotalBsmtSF" -> averageTotalBsmtSF,
      "BedroomAbvGr" -> averageBedroomAbvGr,
      "YearBuilt" -> averageYearBuilt,
      "GarageCars" -> averageGarageCars,
      "2ndFlrSF" -> average2ndFlrSF,
      "BsmtUnfSF" -> averageBsmtUnfSF,
      "ScreenPorch" -> averageScreenPorch,
      "HalfBath" -> averageHalfBath,
      "EnclosedPorch" -> averageEnclosedPorch,
      "OpenPorchSF" -> averageOpenPorchSF,
      "BsmtFullBath" -> averageBsmtFullBath,
      "FullBath" -> averageFullBath,
      "Fireplaces" -> averageFireplaces
    ))

    parsedTestData = parsedTestData.na.fill(Map(
      "MasVnrArea" -> averageMasVnrArea,
      "OverallQual" -> averageOverallQual,
      "LotArea" -> averageLotArea,
      "YearRemodAdd" -> averageYearRemodAdd,
      "BsmtFinSF1" -> averageBsmtFinSF1,
      "WoodDeckSF" -> averageWoodDeckSF,
      "GrLivArea" -> averageGrLivArea,
      "KitchenAbvGr" -> averageKitchenAbvGr,
      "TotalBsmtSF" -> averageTotalBsmtSF,
      "BedroomAbvGr" -> averageBedroomAbvGr,
      "YearBuilt" -> averageYearBuilt,
      "GarageCars" -> averageGarageCars,
      "2ndFlrSF" -> average2ndFlrSF,
      "BsmtUnfSF" -> averageBsmtUnfSF,
      "ScreenPorch" -> averageScreenPorch,
      "HalfBath" -> averageHalfBath,
      "EnclosedPorch" -> averageEnclosedPorch,
      "OpenPorchSF" -> averageOpenPorchSF,
      "BsmtFullBath" -> averageBsmtFullBath,
      "FullBath" -> averageFullBath,
      "Fireplaces" -> averageFireplaces
    ))

    parsedTrainData = parsedTrainData
      .na.fill("unknown",Array("GarageCond"))
      .na.fill("unknown",Array("RoofStyle"))
      .na.fill("unknown",Array("BsmtQual"))
      .na.fill("unknown",Array("Heating"))
      .na.fill("unknown",Array("Functional"))
      .na.fill("unknown",Array("SaleType"))
      .na.fill("unknown",Array("Condition1"))
      .na.fill("unknown",Array("RoofMatl"))
      .na.fill("unknown",Array("KitchenQual"))
      .na.fill("unknown",Array("GarageType"))
      .na.fill("unknown",Array("BsmtCond"))
      .na.fill("unknown",Array("ExterQual"))
      .na.fill("unknown",Array("LotShape"))
      .na.fill("unknown",Array("Condition2"))
      .na.fill("unknown",Array("Neighborhood"))
      .na.fill("unknown",Array("ExterCond"))
      .na.fill("unknown",Array("CentralAir"))
      .na.fill("unknown",Array("MSZoning"))
      .na.fill("unknown",Array("LotFrontage"))
      .na.fill("unknown",Array("LotConfig"))
      .na.fill("unknown",Array("GarageFinish"))
      .na.fill("unknown",Array("PavedDrive"))
      .na.fill("unknown",Array("Exterior1st"))
      .na.fill("unknown",Array("Street"))
      .na.fill("unknown",Array("Electrical"))
      .na.fill("unknown",Array("HeatingQC"))
      .na.fill("unknown",Array("SaleCondition"))
      .na.fill("unknown",Array("Foundation"))
      .na.fill("unknown",Array("LandSlope"))
      .na.fill("unknown",Array("BsmtExposure"))
      .na.fill("unknown",Array("GarageQual"))
      .na.fill("unknown",Array("BldgType"))
      .na.fill("unknown",Array("BsmtFinType1"))
      .na.fill("unknown",Array("Exterior2nd"))
      .na.fill("unknown",Array("Utilities"))
      .na.fill("unknown",Array("LandContour"))
      .na.fill("unknown",Array("BsmtFinType2"))
      .na.fill("unknown",Array("HouseStyle"))
      .na.fill("unknown",Array("MasVnrType"))

    parsedTestData = parsedTestData
      .na.fill("unknown",Array("GarageCond"))
      .na.fill("unknown",Array("RoofStyle"))
      .na.fill("unknown",Array("BsmtQual"))
      .na.fill("unknown",Array("Heating"))
      .na.fill("unknown",Array("Functional"))
      .na.fill("unknown",Array("SaleType"))
      .na.fill("unknown",Array("Condition1"))
      .na.fill("unknown",Array("RoofMatl"))
      .na.fill("unknown",Array("KitchenQual"))
      .na.fill("unknown",Array("GarageType"))
      .na.fill("unknown",Array("BsmtCond"))
      .na.fill("unknown",Array("ExterQual"))
      .na.fill("unknown",Array("LotShape"))
      .na.fill("unknown",Array("Condition2"))
      .na.fill("unknown",Array("Neighborhood"))
      .na.fill("unknown",Array("ExterCond"))
      .na.fill("unknown",Array("CentralAir"))
      .na.fill("unknown",Array("MSZoning"))
      .na.fill("unknown",Array("LotFrontage"))
      .na.fill("unknown",Array("LotConfig"))
      .na.fill("unknown",Array("GarageFinish"))
      .na.fill("unknown",Array("PavedDrive"))
      .na.fill("unknown",Array("Exterior1st"))
      .na.fill("unknown",Array("Street"))
      .na.fill("unknown",Array("Electrical"))
      .na.fill("unknown",Array("HeatingQC"))
      .na.fill("unknown",Array("SaleCondition"))
      .na.fill("unknown",Array("Foundation"))
      .na.fill("unknown",Array("LandSlope"))
      .na.fill("unknown",Array("BsmtExposure"))
      .na.fill("unknown",Array("GarageQual"))
      .na.fill("unknown",Array("BldgType"))
      .na.fill("unknown",Array("BsmtFinType1"))
      .na.fill("unknown",Array("Exterior2nd"))
      .na.fill("unknown",Array("Utilities"))
      .na.fill("unknown",Array("LandContour"))
      .na.fill("unknown",Array("BsmtFinType2"))
      .na.fill("unknown",Array("HouseStyle"))
      .na.fill("unknown",Array("MasVnrType"))

    // one hot encoder
    val indexers = oneHotEncodeFeatures.map(c =>
      new StringIndexer()
        .setHandleInvalid("skip")
        .setInputCol(c)
        .setOutputCol(c + "_idx")
    )
    val encoders = oneHotEncodeFeatures.map(c =>
      new OneHotEncoderEstimator()
        .setInputCols(Array(c + "_idx"))
        .setOutputCols(Array(c + "_vec"))
    )
    val assembler = new VectorAssembler()
      .setInputCols(featuresName)
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val pipeline =
      new Pipeline().setStages(indexers ++ encoders ++ Array(assembler))
    var transformedTrainData = pipeline
      .fit(parsedTrainData)
      .transform(parsedTrainData)
    var transformedTestData = pipeline
      .fit(parsedTrainData)
      .transform(parsedTestData)

    transformedTrainData =
      transformedTrainData.select(col("SalePrice").as("label"), col("features"))
    transformedTestData = transformedTestData.select("features")

    (transformedTrainData, transformedTestData)
  }

  def trainAndPredict(
                       trainData: DataFrame,
                       testData: DataFrame
                     ): DataFrame = {
    var lr = new LinearRegression()
    var lrModel = lr.fit(trainData)

    lrModel.transform(testData)
  }

//  def write2CSV(
//                 prediction: DataFrame,
//                 testData: DataFrame,
//                 outputDir: String,
//                 isWrite: Boolean
//               ): DataFrame = {
//    var df1 = testData
//      .withColumn("id", monotonically_increasing_id())
//      .select("Id", "id")
//
//    df1.printSchema()
//    var df2 = prediction
//      .withColumn("id", monotonically_increasing_id())
//      .select(col("id"), col("prediction").as("SalePrice"))
//    //df2 = df2.withColumn("SalePrice", df2("SalePrice").cast(IntegerType))
//    val res = df1
//      .join(df2, df1("id") === df2("id"), "outer")
//      .drop("id")
//      .orderBy(asc("Id"))
//    res.show()
//    if (isWrite) {
//
//      res.coalesce(1)
//        .write
//        .format("csv")
//        .option("header", "True")
//        .mode("overwrite")
//        .csv(outputDir)
//    }
//    res
//  }

}
