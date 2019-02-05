package fr.snasello.datas.es;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.gson.Gson;

import fr.snasello.datas.es.ElasticSearchRepository;
import fr.snasello.datas.model.Page;
import fr.snasello.datas.model.PageData;
import fr.snasello.datas.model.Sort;
import fr.snasello.datas.model.SortDirection;

@Testcontainers
public class ElasticSearchRepositoryITest {

    @Container
    private static final ElasticsearchContainer ES_CONTAINER = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:6.4.1");

    private static final Gson gson = new Gson();
    
    private static final String TEST_INDEX = "test";
    
    private static final String TEST_ALIAS_INDEX = "test-index";
    
    private static final String TEST_ALIAS_SEARCH = "test-search";
    
    private static final String TEST_TYPE = "TestType";
    
    @Test
    public void saveUpdateDelete() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		String objId = repository.save(null, testObject("this is a name"), jsonMapper());
    		GetRequest getRequest = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId);
    		Map<String,Object> source = esClient.get(getRequest).getSource();
    		assertEquals("this is a name", source.get("name"));
    		
    		String objId2 = repository.save(null, testObject("this is a name 2"), false, jsonMapper());
    		GetRequest getRequest2 = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId2);
    		Map<String,Object> source2 = esClient.get(getRequest2).getSource();
    		assertEquals("this is a name 2", source2.get("name"));
    		
    		repository.update(objId, testObject("name update"), jsonMapper());
    		GetRequest getRequestUpdate = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId);
    		Map<String,Object> sourceUpdate = esClient.get(getRequestUpdate).getSource();
    		assertEquals("name update", sourceUpdate.get("name"));
    		
    		repository.update(objId, testObject("name update 2"), false, jsonMapper());
    		GetRequest getRequestUpdate2 = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId);
    		Map<String,Object> sourceUpdate2 = esClient.get(getRequestUpdate2).getSource();
    		assertEquals("name update 2", sourceUpdate2.get("name"));
    		
    		repository.delete(objId);
    		GetRequest getRequestDelete = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId);
    		GetResponse getResponseDelete = esClient.get(getRequestDelete);
    		assertFalse(getResponseDelete.isExists());
    		
    		repository.delete(objId2, false);
    		GetRequest getRequestDelete2 = new GetRequest(TEST_ALIAS_INDEX, TEST_TYPE, objId2);
    		GetResponse getResponseDelete2 = esClient.get(getRequestDelete2);
    		assertFalse(getResponseDelete2.isExists());
    	}
    }
    
    @Test
    public void searchById() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, "test");
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    	
    		Optional<TestObject> testObject1 = repository.searchById("123456", hitMapper());
    		assertFalse(testObject1.isPresent());
    		
    		String id2 = repository.save(null, testObject("test name"), jsonMapper());
    		Optional<TestObject> testObject2 = repository.searchById(id2, hitMapper());
    		assertTrue(testObject2.isPresent());
    	}
    }
    
    @Test
    public void searchTestSort() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		PageData<TestObject> datasASC = repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), Arrays.asList(new Sort("name", SortDirection.ASC)), hitMapper());
    		assertNotNull(datasASC);
    		PageData<TestObject> datasDSC = repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), Arrays.asList(new Sort("name", SortDirection.DSC)), hitMapper());
    		assertNotNull(datasDSC);
    	}
    }
    
    @Test
    public void searchPageDataWithNoDatas() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		PageData<TestObject> datas = repository.searchPageData(QueryBuilders.matchAllQuery(), Page.firstPageWithLimit(10), Arrays.asList(new Sort("name", SortDirection.ASC)), hitMapper());
    		assertNotNull(datas);

    		assertEquals(10, datas.getPage().get().getLimit());
    		assertEquals(0, datas.getPage().get().getOffset());
    		assertEquals(0, datas.getTotalSize());
    		
    		assertNotNull(datas.getDatas());
    		assertTrue(datas.getDatas().isEmpty());
    	}
    }
    
    @Test
    public void searchPageDataWithDatas() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		IntStream.range(1, 17).forEach(i -> {
				try {
					repository.save(null, testObject("name " + i), true, jsonMapper());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
    		
    		Page page = Page.firstPageWithLimit(10);
    		PageData<TestObject> datasPage1 = repository.searchPageData(QueryBuilders.matchAllQuery(), page, Collections.emptyList(), hitMapper());
    		assertNotNull(datasPage1);

    		assertEquals(10, datasPage1.getPage().get().getLimit());
    		assertEquals(0, datasPage1.getPage().get().getOffset());
    		assertEquals(16, datasPage1.getTotalSize());
    		
    		assertNotNull(datasPage1.getDatas());
    		assertFalse(datasPage1.getDatas().isEmpty());
    		assertEquals(10, datasPage1.getDatas().size());
    		
    		PageData<TestObject> datasPage2 = repository.searchPageData(QueryBuilders.matchAllQuery(), page.nextPage(), Collections.emptyList(), hitMapper());
    		assertEquals(6, datasPage2.getDatas().size());
    		assertEquals(10, datasPage2.getPage().get().getOffset());
    		assertEquals(16, datasPage2.getTotalSize());
    	}
    }
    
    @Test
    public void searchScrollWithNoDatas() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		List<TestObject> datas = repository.searchScroll(QueryBuilders.matchAllQuery(), Collections.emptyList(), hitMapper());
    		assertNotNull(datas);
    		assertEquals(0, datas.size());
    	}
    }
    
    @Test
    public void searchScrollWithDatas() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		IntStream.range(1, 17).forEach(i -> {
				try {
					repository.save(null, testObject("name " + i), true, jsonMapper());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
    		
    		List<TestObject> datas = repository.searchScroll(QueryBuilders.matchAllQuery(), Collections.emptyList(), hitMapper());
    		assertNotNull(datas);
    		assertEquals(16, datas.size());
    	}
    }
    
    @Test
    public void bulkSaveEmpty() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		List<TestObject> objects = new ArrayList<>();
    		Optional<BulkResponse> responses = repository.bulkSave(objects, jsonMapper());
    		assertFalse(responses.isPresent());
    	}	
    }
    
    @Test
    public void bulkSave() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		List<TestObject> objects = IntStream.range(1, 17).mapToObj(i -> testObject("name " + i)).collect(Collectors.toList());
    		Optional<BulkResponse> responses = repository.bulkSave(objects, jsonMapper());
    		assertTrue(responses.isPresent());
    		assertFalse(responses.get().hasFailures());
    		
    		StringBuilder sb = new StringBuilder();
    		repository.bulkLogFailure(responses, sb::append);
    		assertEquals(sb.toString(), "");
    	}	
    }
    
    @Test
    public void bulkSaveWithFailures() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);
    		
    		List<TestObject> objects = IntStream.range(1, 2).mapToObj(i -> testObject(i, "name " + i)).collect(Collectors.toList());
    		objects.add(testObject("name " + 9));
    		Optional<BulkResponse> responses = repository.bulkSave(objects, jsonMapper());
    		assertTrue(responses.isPresent());
    		assertTrue(responses.get().hasFailures());
    		
    		StringBuilder sb = new StringBuilder();
    		repository.bulkLogFailure(responses, sb::append);
    		assertEquals(sb.toString(), "ElasticsearchException[Elasticsearch exception [type=strict_dynamic_mapping_exception, reason=mapping set to strict, dynamic introduction of [id] within [TestType] is not allowed]]");
    	}	
    }
    
    @Test
    public void executeBulkEmpty() throws IOException {
    	try(RestHighLevelClient esClient = esClient()){
    		cleanAndCreateIndex(esClient, TEST_INDEX);
    		ElasticSearchRepository repository = repository(esClient, TEST_ALIAS_INDEX, TEST_ALIAS_SEARCH, TEST_TYPE);

    		Optional<BulkResponse> responses = repository.bulkSave(Collections.emptyList(), jsonMapper());
    		assertFalse(responses.isPresent());
    	}	
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
    
    private TestObject testObject(int id, String name) {
    	TestObject to = new TestObject();
    	to.setId(Integer.toString(id));
    	to.setName(name);
    	return to;
    }
    
    private TestObject testObject(String name) {
    	TestObject to = new TestObject();
    	to.setName(name);
    	return to;
    }
    
    private ElasticSearchRepository repository(
    		final RestHighLevelClient esClient,
    		final String indexNameIndex,
    		final String indexNameSearch,
    		final String typeName) {
    	
    	return new ElasticSearchRepository(esClient, () -> indexNameIndex, () -> indexNameSearch, typeName, true);
    }
    
    private void cleanAndCreateIndex(
    		RestHighLevelClient esClient, 
    		String indexName) throws IOException {
    	
    	GetIndexRequest getIndexRequest = new GetIndexRequest();
    	getIndexRequest.indices(indexName);
    	if(esClient.indices().exists(getIndexRequest)) {
    		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
    		esClient.indices().delete(deleteIndexRequest);
    	}
		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		createIndexRequest.source(getSettings(indexName), XContentType.JSON);
		esClient.indices().create(createIndexRequest);
    }
    
    private String getSettings(String indexName){
    	InputStream inputStream = ElasticSearchRepositoryITest.class.getResourceAsStream(indexName + "-settings.json");
    	try(BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))){
    		return reader.lines()
    			   .parallel().collect(Collectors.joining("\n"));
    	} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }
    
    private RestHighLevelClient esClient() {
    	return new RestHighLevelClient(restClientBuilder());
    }
    
    private RestClientBuilder restClientBuilder() {
    	return RestClient.builder(new HttpHost(ES_CONTAINER.getContainerIpAddress(), ES_CONTAINER.getMappedPort(9200), "http"));
    }

}
