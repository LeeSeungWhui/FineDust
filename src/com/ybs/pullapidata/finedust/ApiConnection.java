package com.ybs.pullapidata.finedust;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

public class ApiConnection
{
	private String Url;
	public StringBuilder urlBuilder;	
	Document result ;
	Elements elements;
	
	public List<String> getResult(String attribute) {
		List<String> resultArray = new ArrayList<String>();
		elements = result.select(attribute);
		for (Element e : elements) 
		{
           resultArray.add(e.text());
        }
		
		return resultArray;
	}

	public void setServiceKey(String keyName, String serviceKey) throws UnsupportedEncodingException {
		urlBuilder.append("?" + URLEncoder.encode(keyName, "UTF-8") + "=" + serviceKey);
	}
	
	public String getUrl() {
		return Url;
	}

	public void setUrl(String url) {
		Url = url;
		urlBuilder = new StringBuilder(Url);
	}
	
	public void urlAppender(String param, String val) throws IOException
	{
		urlBuilder.append("&" + URLEncoder.encode(param, "UTF-8") + "=" + URLEncoder.encode(val, "UTF-8")); 
	}
	
	public void urlAppenderNoTrans(String param, String val) throws IOException
	{
		urlBuilder.append("&" + URLEncoder.encode(param, "UTF-8") + "=" + val); 
	}
	
	public void pullData()
	{
		try {
			result = Jsoup.connect(new String(urlBuilder)).maxBodySize(0).parser(Parser.xmlParser()).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pullData();
		}
	}
	
	public void addData()
	{
		try {
			result.appendChild(Jsoup.connect(new String(urlBuilder)).maxBodySize(0).parser(Parser.xmlParser()).get());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pullData();
		}
	}
	
	ApiConnection()
	{
		/* blank */
	}
}
