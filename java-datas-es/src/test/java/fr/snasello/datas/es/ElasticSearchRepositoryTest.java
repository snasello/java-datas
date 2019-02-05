package fr.snasello.datas.es;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import fr.snasello.datas.model.Page;

public class ElasticSearchRepositoryTest {

    private static final Gson gson = new Gson();
    
    private static final String TEST_ALIAS_INDEX = "test-index";
    
    private static final String TEST_ALIAS_SEARCH = "test-search";
    
    private static final String TEST_TYPE = "TestType";
    
    @Test
    public void save() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.save("1234", testObject("name 1"), jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", null, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", testObject("name 1"), null));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", testObject("name 1"), e -> null));
    		
    		assertThrows(IOException.class, () -> repository.save("1234", testObject("name 1"), true, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", null, true, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", testObject("name 1"), true, null));
    		assertThrows(NullPointerException.class, () -> repository.save("1234", testObject("name 1"), true, e -> null));
    	}
    }
    
    @Test
    public void update() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.update("1234", testObject("name 1"), jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update(null, testObject("name 1"), jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", null, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", testObject("name 1"), null));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", testObject("name 1"), e -> null));
    		
    		assertThrows(IOException.class, () -> repository.update("1234", testObject("name 1"), true, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update(null, testObject("name 1"), true, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", null, true, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", testObject("name 1"), true, null));
    		assertThrows(NullPointerException.class, () -> repository.update("1234", testObject("name 1"), true, e -> null));
    	}
    }
    
    @Test
    public void delete() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.delete("1234"));
    		assertThrows(NullPointerException.class, () -> repository.delete(null));
    		
    		assertThrows(IOException.class, () -> repository.delete("1234", true));
    		assertThrows(NullPointerException.class, () -> repository.delete(null, true));
    	}
    }
    
    @Test
    public void searchById() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.searchById("1234", hitMapper()));
    		assertThrows(NullPointerException.class, () -> repository.searchById(null, hitMapper()));
    		assertThrows(NullPointerException.class, () -> repository.searchById("1234", null));
    	}
    }
    
    @Test
    public void searchPageData() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), Collections.emptyList(), hitMapper()));
    		assertThrows(NullPointerException.class, () -> repository.searchPageData(null, null, null, null));
    		assertThrows(NullPointerException.class, () -> repository.searchPageData(QueryBuilders.matchAllQuery(), null, null, null));
    		assertThrows(NullPointerException.class, () -> repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), null, null));
    		assertThrows(NullPointerException.class, () -> repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), Collections.emptyList(), null));
    	}
    }
    
    @Test
    public void searchScroll() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(IOException.class, () -> repository.searchScroll(QueryBuilders.matchAllQuery(), Collections.emptyList(), hitMapper()));
    		assertThrows(NullPointerException.class, () -> repository.searchScroll(null, null, null));
    		assertThrows(NullPointerException.class, () -> repository.searchScroll(QueryBuilders.matchAllQuery(), null, null));
    		assertThrows(NullPointerException.class, () -> repository.searchScroll(QueryBuilders.matchAllQuery(), Collections.emptyList(), null));
    	}
    }
    
    @Test
    public void bulkSave() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		List<TestObject> objects = IntStream.range(1, 17).mapToObj(i -> testObject("name " + i)).collect(Collectors.toList());
    		assertThrows(IOException.class, () -> repository.bulkSave(objects, jsonMapper()));
    		assertThrows(NullPointerException.class, () -> repository.bulkSave(null, null));
    		assertThrows(NullPointerException.class, () -> repository.bulkSave(Collections.emptyList(), null));
    	}
    }
    
    @Test
    public void executeBulk() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		assertThrows(NullPointerException.class, () -> repository.executeBulk(null));
    		BulkRequest bulkRequest = new BulkRequest();
    		assertFalse(repository.executeBulk(bulkRequest).isPresent());
    		IndexRequest indexRequest = new IndexRequest(TEST_ALIAS_INDEX, TEST_TYPE);
    		indexRequest.source("{}", XContentType.JSON);
    		bulkRequest.add(indexRequest);
    		assertThrows(IOException.class, () -> repository.executeBulk(bulkRequest));
    	}
    }
    
    private TestObject testObject(String name) {
    	TestObject to = new TestObject();
    	to.setName(name);
    	return to;
    }
    
    private Function<SearchHit, TestObject> hitMapper() {
    	return hit -> {
    		TestObject to = gson.fromJson(hit.getSourceAsString(), TestObject.class);
    		to.setId(hit.getId());
    		return to;
    	};
    }
    
    private Function<TestObject, String> jsonMapper(){
    	return e -> gson.toJson(e);
    }
    
    private ElasticSearchRepository repository(
    		final RestHighLevelClient esClient,
    		final String indexNameIndex,
    		final String indexNameSearch,
    		final String typeName) {
    	
    	return new ElasticSearchRepository(esClient, () -> indexNameIndex, () -> indexNameSearch, typeName, true);
    }
    
    private RestHighLevelClient esClient() {
    	return new RestHighLevelClient(restClientBuilder());
    }
    
    private RestClientBuilder restClientBuilder() {
    	return RestClient.builder(new HttpHost("localhost", 9999, "http"));
    }
}
