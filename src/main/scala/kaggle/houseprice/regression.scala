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
import org.apache.spark.sql.Row
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator

object HPRegression {

  def main(args: Array[String]):Unit={

    val spark = SparkSession.builder
      .appName("Kaggle-House-Price-Regression")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val trainDataDir = args(0)
    val testDataDir = args(1)
    val submissionDataDir = args(2)
    val outputDataDir = args(3)

    val trainData = loadData(spark = spark, fileDir = trainDataDir)
    val testData = loadData(spark = spark, fileDir = testDataDir)
    val sampleSubmission = loadData(spark = spark, fileDir = submissionDataDir)

    val (parsedTrainData, parsedTestData) = parseData(
      trainData = trainData,
      testData = testData
    )

    val prediction = trainAndPredict(
      trainData = parsedTrainData,
      testData = parsedTestData
    )

    val res = write2CSV(
      prediction = prediction,
      sampleSubmission = sampleSubmission,
      outputDir = outputDataDir,
      isWrite = true
    )
  }

  def loadData(spark: SparkSession,
                fileDir: String
              ): (DataFrame) = {

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema","true")
      .load(fileDir)
    df
  }

  def parseData(
                 trainData: DataFrame,
                 testData: DataFrame
               ): (DataFrame, DataFrame) = {
    var parsedTrainData = trainData
    var parsedTestData = testData

    val dropFeatures = List(
      "Id","1stFlrSF","TotRmsAbvGrd","GarageArea", "PoolArea","MoSold","3SsnPorch",
      "BsmtFinSF2","BsmtHalfBath", "MiscVal","LowQualFinSF","YrSold","OverallCond","MSSubClass",
      "Alley", "LotFrontage","BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinType2",
      "FireplaceQu","GarageType","GarageYrBlt","GarageFinish","GarageQual","GarageCond","PoolQC",
      "Fence","MiscFeature","MSZoning", "Utilities","Functional","Exterior1st", "Exterior2nd",
      "MasVnrType","KitchenQual","SaleType", "MasVnrArea","BsmtFinSF1","TotalBsmtSF","GarageCars",
      "BsmtUnfSF","BsmtFullBath")

    val oneHotEncodeFeatures = Array(
      "RoofStyle", "Heating", "Condition1", "RoofMatl", "ExterQual", "LotShape", "Condition2",
      "Neighborhood", "ExterCond", "CentralAir", "LotConfig", "PavedDrive", "Street", "Electrical",
      "HeatingQC", "SaleCondition", "Foundation", "LandSlope",  "BldgType",  "LandContour", "HouseStyle")

    val featuresName = Array(
      "OverallQual", "LotArea", "YearRemodAdd", "WoodDeckSF", "GrLivArea", "KitchenAbvGr",
      "BedroomAbvGr", "YearBuilt", "2ndFlrSF", "ScreenPorch", "HalfBath", "EnclosedPorch",
      "OpenPorchSF", "FullBath", "Fireplaces", "RoofStyle_vec", "Heating_vec", "Condition1_vec",
      "RoofMatl_vec", "ExterQual_vec", "LotShape_vec", "Condition2_vec", "Neighborhood_vec",
      "ExterCond_vec", "CentralAir_vec", "LotConfig_vec", "PavedDrive_vec", "Street_vec",
      "Electrical_vec", "HeatingQC_vec", "SaleCondition_vec", "Foundation_vec", "LandSlope_vec",
      "BldgType_vec",  "LandContour_vec", "HouseStyle_vec")

    val averageOverallQual= parsedTrainData.select(mean(parsedTrainData("OverallQual"))).first().getDouble(0)
    val averageLotArea= parsedTrainData.select(mean(parsedTrainData("LotArea"))).first().getDouble(0)
    val averageYearRemodAdd= parsedTrainData.select(mean(parsedTrainData("YearRemodAdd"))).first().getDouble(0)
    val averageWoodDeckSF= parsedTrainData.select(mean(parsedTrainData("WoodDeckSF"))).first().getDouble(0)
    val averageGrLivArea= parsedTrainData.select(mean(parsedTrainData("GrLivArea"))).first().getDouble(0)
    val averageKitchenAbvGr= parsedTrainData.select(mean(parsedTrainData("KitchenAbvGr"))).first().getDouble(0)
    val averageBedroomAbvGr= parsedTrainData.select(mean(parsedTrainData("BedroomAbvGr"))).first().getDouble(0)
    val averageYearBuilt= parsedTrainData.select(mean(parsedTrainData("YearBuilt"))).first().getDouble(0)
    val average2ndFlrSF= parsedTrainData.select(mean(parsedTrainData("2ndFlrSF"))).first().getDouble(0)
    val averageScreenPorch= parsedTrainData.select(mean(parsedTrainData("ScreenPorch"))).first().getDouble(0)
    val averageHalfBath= parsedTrainData.select(mean(parsedTrainData("HalfBath"))).first().getDouble(0)
    val averageEnclosedPorch= parsedTrainData.select(mean(parsedTrainData("EnclosedPorch"))).first().getDouble(0)
    val averageOpenPorchSF= parsedTrainData.select(mean(parsedTrainData("OpenPorchSF"))).first().getDouble(0)
    val averageFullBath= parsedTrainData.select(mean(parsedTrainData("FullBath"))).first().getDouble(0)
    val averageFireplaces= parsedTrainData.select(mean(parsedTrainData("Fireplaces"))).first().getDouble(0)

    // drop unused columns
    for (feature <- dropFeatures) {
      parsedTrainData = parsedTrainData.drop(feature)
      parsedTestData = parsedTestData.drop(feature)
    }

    // fill null numeric values
    parsedTrainData = parsedTrainData.na.fill(Map(
      "OverallQual" -> averageOverallQual,
      "LotArea" -> averageLotArea,
      "YearRemodAdd" -> averageYearRemodAdd,
      "WoodDeckSF" -> averageWoodDeckSF,
      "GrLivArea" -> averageGrLivArea,
      "KitchenAbvGr" -> averageKitchenAbvGr,
      "BedroomAbvGr" -> averageBedroomAbvGr,
      "YearBuilt" -> averageYearBuilt,
      "2ndFlrSF" -> average2ndFlrSF,
      "ScreenPorch" -> averageScreenPorch,
      "HalfBath" -> averageHalfBath,
      "EnclosedPorch" -> averageEnclosedPorch,
      "OpenPorchSF" -> averageOpenPorchSF,
      "FullBath" -> averageFullBath,
      "Fireplaces" -> averageFireplaces
    ))

    parsedTestData = parsedTestData.na.fill(Map(
      "OverallQual" -> averageOverallQual,
      "LotArea" -> averageLotArea,
      "YearRemodAdd" -> averageYearRemodAdd,
      "WoodDeckSF" -> averageWoodDeckSF,
      "GrLivArea" -> averageGrLivArea,
      "KitchenAbvGr" -> averageKitchenAbvGr,
      "BedroomAbvGr" -> averageBedroomAbvGr,
      "YearBuilt" -> averageYearBuilt,
      "2ndFlrSF" -> average2ndFlrSF,
      "ScreenPorch" -> averageScreenPorch,
      "HalfBath" -> averageHalfBath,
      "EnclosedPorch" -> averageEnclosedPorch,
      "OpenPorchSF" -> averageOpenPorchSF,
      "FullBath" -> averageFullBath,
      "Fireplaces" -> averageFireplaces
    ))

    // fill null categorical values
    parsedTrainData = parsedTrainData
      .na.fill("unknown",Array("RoofStyle"))
      .na.fill("unknown",Array("Heating"))
      .na.fill("unknown",Array("Condition1"))
      .na.fill("unknown",Array("RoofMatl"))
      .na.fill("unknown",Array("ExterQual"))
      .na.fill("unknown",Array("LotShape"))
      .na.fill("unknown",Array("Condition2"))
      .na.fill("unknown",Array("Neighborhood"))
      .na.fill("unknown",Array("ExterCond"))
      .na.fill("unknown",Array("CentralAir"))
      .na.fill("unknown",Array("LotConfig"))
      .na.fill("unknown",Array("PavedDrive"))
      .na.fill("unknown",Array("Street"))
      .na.fill("unknown",Array("Electrical"))
      .na.fill("unknown",Array("HeatingQC"))
      .na.fill("unknown",Array("SaleCondition"))
      .na.fill("unknown",Array("Foundation"))
      .na.fill("unknown",Array("LandSlope"))
      .na.fill("unknown",Array("BldgType"))
      .na.fill("unknown",Array("LandContour"))
      .na.fill("unknown",Array("HouseStyle"))

    parsedTestData = parsedTestData
      .na.fill("unknown",Array("RoofStyle"))
      .na.fill("unknown",Array("Heating"))
      .na.fill("unknown",Array("Condition1"))
      .na.fill("unknown",Array("RoofMatl"))
      .na.fill("unknown",Array("ExterQual"))
      .na.fill("unknown",Array("LotShape"))
      .na.fill("unknown",Array("Condition2"))
      .na.fill("unknown",Array("Neighborhood"))
      .na.fill("unknown",Array("ExterCond"))
      .na.fill("unknown",Array("CentralAir"))
      .na.fill("unknown",Array("LotConfig"))
      .na.fill("unknown",Array("PavedDrive"))
      .na.fill("unknown",Array("Street"))
      .na.fill("unknown",Array("Electrical"))
      .na.fill("unknown",Array("HeatingQC"))
      .na.fill("unknown",Array("SaleCondition"))
      .na.fill("unknown",Array("Foundation"))
      .na.fill("unknown",Array("LandSlope"))
      .na.fill("unknown",Array("BldgType"))
      .na.fill("unknown",Array("LandContour"))
      .na.fill("unknown",Array("HouseStyle"))

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

    transformedTrainData = transformedTrainData
        .select(col("SalePrice").as("label"), col("features"))

    transformedTestData = transformedTestData.select("features")

    (transformedTrainData, transformedTestData)
  }

  def trainAndPredict(
                       trainData: DataFrame,
                       testData: DataFrame
                     ): DataFrame = {
    var lr = new LinearRegression() //RandomForestRegressor()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 1.0))
      .addGrid(lr.elasticNetParam, Array(1.0))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(
        new RegressionEvaluator()
      )
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    var lrModel = cv.fit(trainData)
    var lrPredictions = lrModel.transform(testData)

    lrPredictions

  }

  def write2CSV(
                 prediction: DataFrame,
                 sampleSubmission: DataFrame,
                 outputDir: String,
                 isWrite: Boolean
               ): DataFrame = {

    val submissionId = sampleSubmission//.drop("SalePrice")
      .withColumn("create_id", monotonically_increasing_id())
      .select("Id", "create_id")

    val predictionPrice = prediction
      .withColumn("create_id", monotonically_increasing_id())
      .select(col("create_id"), col("prediction").as("SalePrice"))

    val res = submissionId
      .join(predictionPrice, submissionId("create_id") === predictionPrice("create_id"), "inner")
      .drop("create_id")
      .orderBy(asc("Id"))

    if (isWrite) {
      res.coalesce(1)
        .write
        .format("csv")
        .option("header", "True")
        .mode("overwrite")
        .csv(outputDir)
    }

    res
  }

}
