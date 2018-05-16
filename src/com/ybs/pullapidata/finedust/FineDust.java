package com.ybs.pullapidata.finedust;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ybs.pullapidata.finedust.ApiConnection;
import com.ybs.pullapidata.finedust.DbConnection;

public class FineDust 
{
	static public ApiConnection apiconnection;
	static public String BaseDate, BaseTime;
	static public String tableName = "FINE_DUST";
	
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException 
	{
		// 기본 설정
		long time = System.currentTimeMillis();
		SimpleDateFormat _date = new SimpleDateFormat("YYYYMMdd");
		SimpleDateFormat _time = new SimpleDateFormat("HHmmss");
		BaseDate = _date.format(new Date(time));
		BaseTime = _time.format(new Date( time));
		List<String> column = new ArrayList<String>();
		column.add("seq");
		column.add("station_nm");
		column.add("dataTime");
		column.add("mangName");
		column.add("pm10Value");
		column.add("pm25Value");
		column.add("khaiValue");
		column.add("pm10Grade");
		column.add("pm25Grade");
		column.add("khaiGrade");
		
		// DB 연결
		String host = "192.168.0.53";
		String name = "HVI_DB";
		String user = "root";
		String pass = "dlatl#001";
		DbConnection dbconnection = new DbConnection(host, name, user, pass);
	    dbconnection.Connect();
	    String sql = "";
	    
	    // sequence 받아오기
	    int seq = 1;
		try {
			sql = "Select max(SEQ) as M from " + tableName;
			dbconnection.runQuery(sql);
			dbconnection.getResult().next();
			seq = dbconnection.getResult().getInt("M") + 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}
		
		// 측정소 목록 받아오기
		List<String> msrstnList = new ArrayList<String>();
		sql = "select STATION_NM from MSRSTN_LIST";
		dbconnection.runQuery(sql);
		while(dbconnection.getResult().next())
		{
			msrstnList.add(dbconnection.getResult().getString("STATION_NM"));
		}
	    
	    // api data 받아서 csv파일 생성
	    String FileName = tableName + "_" + BaseDate + BaseTime + ".csv";
	    BufferedWriter bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter, column);
	    apiconnection = new ApiConnection();
	    for(String msrstn : msrstnList)
	    {
	    	try 
	    	{
	    		apiconnection.setUrl("http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty");
				apiconnection.setServiceKey("serviceKey", "aq%2Bd7pEryGFmGFAAIFv8VQps%2FF5YNIGe4RZX%2F2SW4h1%2BGHoWs6c4M9QptIPsQPZ2yHhm5iBOnoKKS89LJtlDNA%3D%3D");
				apiconnection.urlAppender("numOfRows", "9999");
				apiconnection.urlAppenderNoTrans("stationName", msrstn);
				apiconnection.urlAppender("dataTerm", "DAILY");
				apiconnection.urlAppender("ver", "1.3");
				apiconnection.pullData();
				System.out.println(apiconnection.urlBuilder);
				
				List<List<String>> data = new ArrayList<List<String>>(); // csv파일 쓰기위한 변수
				List<String> date; // 날짜 형식이 다르므로 바꾸기 위한 변수
				data.add(new ArrayList<String>());
				data.add(new ArrayList<String>());
				date = apiconnection.getResult(column.get(2));
				for(int j = 0; j < date.size(); j++)
				{
					date.set(j, date.get(j).replace("-", "").replace(" ", "").replace(":", ""));
				}
				data.add(date);
				for(int j = 3; j < column.size(); j++)
				{
					data.add(apiconnection.getResult(column.get(j)));
				}
				for(int k = 0;  k < data.get(3).size(); k++, seq++)
				{
					data.get(0).add(String.valueOf(seq));
					data.get(1).add(msrstn);
				}
				WriteCSV(bufWriter, data);
			} catch (Exception e) 
	    	{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    bufWriter.close();
	    
	    // DB에 입력
	    sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE " + tableName + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES";
	    dbconnection.LoadLocalData(sql);
	}
	
	public static void CreateCSV(BufferedWriter bufWriter, List<String> Column)
	{
		try
		{
			int i = 0;
			for(; i < Column.size() - 1; i++)
			{
				bufWriter.write("\"" + Column.get(i) + "\",");
			}
			bufWriter.write("\"" + Column.get(i) + "\"");
			bufWriter.newLine();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void WriteCSV(BufferedWriter bufWriter, List<List<String>> datalist) throws IOException
	{
//		System.out.println(datalist.get(0).size() + " " + datalist.get(1).size()+ " " + datalist.get(2).size()+ " " + datalist.get(3).size()+ " " + datalist.get(4).size()+ " " + datalist.get(5).size()+ " " + datalist.get(6).size());
		String buffer = "";
		for(int i = 0; i < datalist.get(0).size(); i++)
		{
			int j = 0;
			for(; j < datalist.size() - 1; j++)
			{
				if(datalist.get(j).get(i).contains("</"))
				{
					buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') ) + "\",";
				}
				else
				{
					buffer += "\"" + datalist.get(j).get(i) + "\",";
				}
			}
			if(datalist.get(j).get(i).contains("</"))
			{
				buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') );
			}
			else
			{
				buffer += "\"" + datalist.get(j).get(i);
			}
			buffer += "\"\n";
		}
		System.out.print(buffer);
		bufWriter.write(buffer);
	}
}
