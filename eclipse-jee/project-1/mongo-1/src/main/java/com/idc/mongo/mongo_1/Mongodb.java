package com.idc.mongo.mongo_1;

import static java.util.Arrays.asList;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class Mongodb {
	private MongoClient m_mongo = null;
	private MongoDatabase m_db = null;

	public Mongodb (String host, int port) {
		m_mongo = new MongoClient(host, port);
	}

	public void getDatabase(String databaseName) {
		m_db = m_mongo.getDatabase(databaseName);
	}

	public void listDatabases() {
		System.out.println(">>> List Databases");
		List<String> dbs = m_mongo.getDatabaseNames();
		for (String db : dbs) {
			System.out.println(db);
		}
		System.out.println("<<< List Databases");
	}

	public void queryAllDocuments (String collectionName) {
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find();
		showQueryResults (iterable);
	}
/*
	public void queryDocuments (String collectionName, String key, String value) {
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(eq(key, value));
		showQueryResults (iterable);
	}
*/
	public void queryDocuments (String collectionName, String key, String value) {
		Document doc = new Document(key, value);
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(doc);
		showQueryResults (iterable);
	}
	public void queryDocuments (String collectionName, String key, int value) {
		Document doc = new Document(key, value);
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(doc);
		showQueryResults (iterable);
	}

	public void queryDocuments (String collectionName, String key, String value, String operator) {
		Document doc = new Document(key, new Document (operator, value));
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(doc);
		showQueryResults (iterable);
	}
	public void queryDocuments (String collectionName, String key, int value, String operator) {
		Document doc = new Document(key, new Document (operator, value));
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(doc);
		showQueryResults (iterable);
	}
	public void queryDocumentsWithSort (String collectionName, String key, int value, String operator) {
		Document doc = new Document(key, new Document (operator, value));
		Document sort = new Document("user.zipcode", 1).append("name", -1);
		FindIterable<Document> iterable = m_db.getCollection(collectionName).find(doc).sort(sort);
		showQueryResults (iterable);
	}
	public void showQueryResults (FindIterable<Document> iterable) {
		iterable.forEach(new Block<Document>() {
			public void apply(final Document document) {
				System.out.println("Document "+document);
			}
		});
	}
	
	public void updateDocumentsSample (String collectionName, String key, int value, String operator,
			String updateKey, String updateValue) {
		Document doc = new Document(key, new Document (operator, value));
		Document updateDoc = new Document ("$set", new Document(updateKey, updateValue));
		UpdateResult result = m_db.getCollection(collectionName).updateMany(doc, updateDoc);
		System.out.println("Updated "+result.getModifiedCount()+" documents from "+collectionName);
	}

	public void removeAllDocumentsByCondition (String collectionName, String key, String value) {
		DeleteResult result = m_db.getCollection(collectionName).deleteMany(new Document(key, value));
		System.out.println("Deleted "+result.getDeletedCount()+" documents from "+collectionName);
	}
	public void removeAllDocuments (String collectionName) {
		DeleteResult result = m_db.getCollection(collectionName).deleteMany(new Document());
		System.out.println("Deleted "+result.getDeletedCount()+" documents from "+collectionName);
	}

	public void dropCollection (String collectionName) {
		m_db.getCollection(collectionName).drop();
	}

	public void createExampleDocument(String collectionName) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		try {
			Document document = 
		        new Document("address",
		                new Document()
		                        .append("street", "2 Avenue")
		                        .append("zipcode", "10075")
		                        .append("building", "1480")
		                        .append("coord", asList(-73.9557413, 40.7720266)))
		                .append("borough", "Manhattan")
		                .append("cuisine", "Italian")
		                .append("grades", asList(
		                        new Document()
		                                .append("date", format.parse("2014-10-01T00:00:00Z"))
		                                .append("grade", "A")
		                                .append("score", 11),
		                        new Document()
		                                .append("date", format.parse("2014-01-16T00:00:00Z"))
		                                .append("grade", "B")
		                                .append("score", 17)))
		                .append("name", "Vella")
		                .append("restaurant_id", "41704620");	
			m_db.getCollection(collectionName).insertOne(document);
		}
		catch (Exception ex) {
			System.out.println("Exception; "+ex.getMessage());
		}
	}
	
	public Document createExampleDocument2(String collectionName, String username, int zip) {
		Document document = 
	        new Document("user",
	                new Document()
	                        .append("street", "2 Avenue")
	                        .append("zipcode", zip)
	                        .append("building", "1480"))
	                .append("group", "group1")
	                .append("language", "Italian")
	                .append("name", username)
	                .append("password", "41704620");	
		return document;
	}

	public void insertDocument(String collectionName, Document document) {
		m_db.getCollection(collectionName).insertOne(document);
	}

	public void insertDocument(String collectionName, List<Document> list) {
		m_db.getCollection(collectionName).insertMany(list);
	}

}
