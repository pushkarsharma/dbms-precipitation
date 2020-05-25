package edu.cs236.dbms_precipitation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Precipitation {
	public static SparkSession sparkSession;

	@SuppressWarnings("resource")
	public static Dataset<Row> annualRecord() throws IOException {
		BufferedReader br = null;
		String st;
		String[] arrOfStr;
		StructField[] structFields = new StructField[] {
				new StructField("STN", DataTypes.StringType, true, Metadata.empty()),
				new StructField("MNTH", DataTypes.StringType, true, Metadata.empty()),
				new StructField("PRCP", DataTypes.DoubleType, true, Metadata.empty()) };
		StructType structType = new StructType(structFields);
		List<Row> rows = new ArrayList<Row>();
		@SuppressWarnings("serial")
		List<String> yearList = new ArrayList<String>() {
			{
//				add("2006");
//				add("2007");
//				add("2008");
				add("2009");
			}
		};
		for (String year : yearList) {
			arrOfStr = null;
			br = new BufferedReader(
					new FileReader(new File("/home/pushkar/Desktop/dbms_project/dataset/" + year + ".txt")));
			while ((st = br.readLine()) != null) {
				arrOfStr = st.split("\\s+", 0);
				String prcp = arrOfStr[arrOfStr.length - 3];
				if ((arrOfStr[arrOfStr.length - 1].equals("FRSHTT")) || (prcp.equals("99.99"))) {
					continue;
				}
				char multiplier = prcp.charAt(prcp.length() - 1);
				prcp = prcp.substring(0, prcp.length() - 1);
				Double prcpDouble = (double) 0;
				if (multiplier == 'A') {
					prcpDouble = Double.parseDouble(prcp) * 4;
				} else if (multiplier == 'B' || multiplier == 'E') {
					prcpDouble = Double.parseDouble(prcp) * 2;
				} else if (multiplier == 'C') {
					prcpDouble = (double) (Double.parseDouble(prcp) * 1.33);
				} else if (multiplier == 'D' || multiplier == 'F' || multiplier == 'G') {
					prcpDouble = Double.parseDouble(prcp) * 4;
				} else if (multiplier == 'H' || multiplier == 'I') {
					prcpDouble = (double) 0;
				}
				rows.add(RowFactory.create(arrOfStr[0], arrOfStr[2].substring(4, 6), prcpDouble));
			}
		}
		return sparkSession.createDataFrame(rows, structType);
	}

	public static void main(String[] args) throws IOException, AnalysisException {
		sparkSession = SparkSession.builder().appName("Example").master("local").getOrCreate();
		Dataset<Row> weatherStations = sparkSession.read().option("delimiter", ",").option("header", "true")
				.option("inferSchema", "true").csv(args[0]);

		weatherStations.createTempView("WEATHER_STATIONS");

		Dataset<Row> records = Precipitation.annualRecord();
		records.createTempView("RECORDS");

		Dataset<Row> stationRecords = sparkSession.sql(
				"SELECT STATE, MNTH, AVG(PRCP) AS AVG_PRCP FROM (SELECT * FROM WEATHER_STATIONS, RECORDS WHERE WEATHER_STATIONS.USAF = RECORDS.STN AND WEATHER_STATIONS.STATE IS NOT NULL) GROUP BY STATE, MNTH ORDER BY STATE, MNTH");
		stationRecords.createTempView("STATION_RECORDS");

		Dataset<Row> minRecords = sparkSession
				.sql("SELECT STATE, MIN(AVG_PRCP) AS AVG_PRCP FROM STATION_RECORDS GROUP BY STATE ORDER BY STATE");
		minRecords.createTempView("MIN_RECORDS");

		Dataset<Row> maxRecords = sparkSession
				.sql("SELECT STATE, MAX(AVG_PRCP) AS AVG_PRCP FROM STATION_RECORDS GROUP BY STATE ORDER BY STATE");
		maxRecords.createTempView("MAX_RECORDS");

		Dataset<Row> minStateRecords = sparkSession.sql(
				"SELECT STATION_RECORDS.STATE, STATION_RECORDS.MNTH, STATION_RECORDS.AVG_PRCP AS AVG_MIN FROM STATION_RECORDS, MIN_RECORDS WHERE STATION_RECORDS.STATE = MIN_RECORDS.STATE AND STATION_RECORDS.AVG_PRCP = MIN_RECORDS.AVG_PRCP ORDER BY STATION_RECORDS.STATE");
		minStateRecords.createTempView("MIN_STATE_RECORDS");
//		System.out.println("------------------MIN_STATE_RECORDS------------------");
//		minStateRecords.show(999);

		Dataset<Row> maxStateRecords = sparkSession.sql(
				"SELECT STATION_RECORDS.STATE, STATION_RECORDS.MNTH, STATION_RECORDS.AVG_PRCP AS AVG_MAX FROM STATION_RECORDS, MAX_RECORDS WHERE STATION_RECORDS.STATE = MAX_RECORDS.STATE AND STATION_RECORDS.AVG_PRCP = MAX_RECORDS.AVG_PRCP ORDER BY STATION_RECORDS.STATE");
		maxStateRecords.createTempView("MAX_STATE_RECORDS"); //
//		System.out.println("------------------MAX_STATE_RECORDS------------------");
//		  maxStateRecords.show(999);

		Dataset<Row> minMaxRecords = sparkSession.sql(
				"SELECT MIN_STATE_RECORDS.STATE, MIN_STATE_RECORDS.MNTH AS MIN_MNTH, MIN_STATE_RECORDS.AVG_MIN, MAX_STATE_RECORDS.MNTH AS MAX_MNTH, MAX_STATE_RECORDS.AVG_MAX FROM MIN_STATE_RECORDS, MAX_STATE_RECORDS WHERE MIN_STATE_RECORDS.STATE = MAX_STATE_RECORDS.STATE");
		minMaxRecords.createTempView("MIN_MAX_RECORDS");
		minMaxRecords.show(999);

	}
}