package com.idc.mongo.mongo_1;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class App {
	private static final String HOST = "localhost";
	private static final int PORT = 27017;
	private static final String DATABASE_NAME = "jv1";
	
	private static final String COLLECTION = "restaurants";

	public static void main(String[] args) {
		test5();
	}

	private static void test1() {
		Mongodb mongodb = new Mongodb(HOST, PORT);
		mongodb.getDatabase(DATABASE_NAME);
		mongodb.listDatabases();
		System.out.println("Create document");
		mongodb.createExampleDocument(COLLECTION);
		System.out.println("Query all documents");
		mongodb.queryAllDocuments(COLLECTION);
		System.out.println("Query");
		mongodb.queryDocuments (COLLECTION, "borough", "Manhattan");
	}
	private static void test2() {
		Mongodb mongodb = new Mongodb(HOST, PORT);
		mongodb.getDatabase(DATABASE_NAME);
		mongodb.queryAllDocuments(COLLECTION);
		mongodb.dropCollection (COLLECTION);
		mongodb.queryAllDocuments(COLLECTION);
	}

	private static void test3() {
		Mongodb mongodb = new Mongodb(HOST, PORT);
		mongodb.getDatabase(DATABASE_NAME);

		System.out.println("Create documents");
		Document document1 = mongodb.createExampleDocument2(COLLECTION, "user1", 12345);
		Document document2 = mongodb.createExampleDocument2(COLLECTION, "user2", 12345);
		mongodb.insertDocument(COLLECTION, document1);
		mongodb.insertDocument(COLLECTION, document2);

		System.out.println("Query all documents");
		mongodb.queryAllDocuments(COLLECTION);
		System.out.println("Query");
		mongodb.queryDocuments (COLLECTION, "name", "user1");
	}
	
	private static void test4() {
		Mongodb mongodb = new Mongodb(HOST, PORT);
		mongodb.getDatabase(DATABASE_NAME);
		mongodb.removeAllDocuments (COLLECTION);

		System.out.println("Create documents");
		Document document1 = mongodb.createExampleDocument2(COLLECTION, "user1", 12345);
		Document document2 = mongodb.createExampleDocument2(COLLECTION, "user2", 12345);
		mongodb.insertDocument(COLLECTION, document1);
		mongodb.insertDocument(COLLECTION, document2);

		System.out.println("Query all documents");
		mongodb.queryAllDocuments(COLLECTION);
		System.out.println("Query");
		mongodb.queryDocuments (COLLECTION, "name", "user1");
	}

	private static void test5() {
		Mongodb mongodb = new Mongodb(HOST, PORT);
		mongodb.getDatabase(DATABASE_NAME);
		mongodb.removeAllDocuments (COLLECTION);

		System.out.println("Create documents");
		Document document1 = mongodb.createExampleDocument2(COLLECTION, "user21", 12345);
		Document document2 = mongodb.createExampleDocument2(COLLECTION, "user22", 36789);
		Document document3 = mongodb.createExampleDocument2(COLLECTION, "user23", 24689);
		Document document4 = mongodb.createExampleDocument2(COLLECTION, "user24", 24689);
		List<Document> list = new ArrayList<Document>();
		list.add(document1);
		list.add(document2);
		list.add(document3);
		list.add(document4);
		mongodb.insertDocument(COLLECTION, list);

		System.out.println("Query all documents");
		mongodb.queryAllDocuments(COLLECTION);

		System.out.println("Query 1");
		mongodb.queryDocuments (COLLECTION, "name", "user23");

		System.out.println("Query 2");
		mongodb.queryDocuments (COLLECTION, "user.zipcode", 36789);

		System.out.println("Query 3");
		mongodb.queryDocuments (COLLECTION, "user.zipcode", 12345, "$gt");

		System.out.println("Query 4");
		mongodb.queryDocuments (COLLECTION, "user.zipcode", 28000, "$lt");

		System.out.println("Query 5");
		mongodb.queryDocumentsWithSort (COLLECTION, "user.zipcode", 88000, "$lt");

		System.out.println("Update 1");
		mongodb.updateDocumentsSample (COLLECTION, "user.zipcode", 88000, "$lt", "group", "newGroup");

		System.out.println("Query all documents");
		mongodb.queryAllDocuments(COLLECTION);
	}
}

/*

*/
