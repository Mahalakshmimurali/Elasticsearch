package com.example.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;

public class ElasticsearchApplication {

    private static RestHighLevelClient client;

    public static void main(String[] args) {
        try {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("localhost", 9200, "http")));

            String vNameCollection = "Hash_Maha";
            String vPhoneCollection = "Hash_9847";

            createCollection(vNameCollection);
            createCollection(vPhoneCollection);

            System.out.println("Employee count: " + getEmpCount(vNameCollection));

            indexData(vNameCollection, "Department");
            indexData(vPhoneCollection, "Gender");

            System.out.println("Employee count after indexing: " + getEmpCount(vNameCollection));

            delEmpById(vNameCollection, "E02003");

            System.out.println("Employee count after deletion: " + getEmpCount(vNameCollection));

            searchByColumn(vNameCollection, "Department", "IT");
            searchByColumn(vNameCollection, "Gender", "Male");

            searchByColumn(vPhoneCollection, "Department", "IT");

            getDepFacet(vNameCollection);
            getDepFacet(vPhoneCollection);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createCollection(String collectionName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(collectionName);
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    public static long getEmpCount(String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        searchRequest.source().size(0); // No need for documents, just count
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getHits().getTotalHits().value;
    }

    public static void indexData(String collectionName, String columnName) {
        try (CSVReader reader = new CSVReader(new FileReader("Downloads/employee_data.csv"))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                int columnIndex = getColumnIndex(columnName);
                if (columnIndex == -1 || columnIndex >= nextLine.length) {
                    System.err.println("Invalid column index for column: " + columnName);
                    continue;
                }
                String value = nextLine[columnIndex];
                IndexRequest indexRequest = new IndexRequest(collectionName)
                        .source(XContentFactory.jsonBuilder()
                                .startObject()
                                .field(columnName, value)
                                .endObject());
                client.index(indexRequest, RequestOptions.DEFAULT);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    private static int getColumnIndex(String columnName) {
        switch (columnName) {
            case "Department":
                return 1; // Adjust based on your CSV structure
            case "Gender":
                return 2; // Adjust based on your CSV structure
            default:
                return -1; // Invalid column name
        }
    }

    public static void delEmpById(String collectionName, String empId) throws IOException {
        DeleteRequest request = new DeleteRequest(collectionName, empId);
        client.delete(request, RequestOptions.DEFAULT);
    }

    public static void searchByColumn(String collectionName, String columnName, String value) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        searchRequest.source().query(QueryBuilders.matchQuery(columnName, value));
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("Search results: " + response.getHits().getHits().length);
    }

    public static void getDepFacet(String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        searchRequest.source().aggregation(AggregationBuilders.terms("departments").field("Department.keyword"));
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        Terms terms = response.getAggregations().get("departments");
        System.out.println("Department facets: " + terms.getBuckets());
    }
}
