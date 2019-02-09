package fr.snasello.datas.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import fr.snasello.datas.model.Page;
import fr.snasello.datas.model.PageData;
import fr.snasello.datas.model.Sort;
import fr.snasello.datas.model.SortDirection;

/**
 * The purpose of this class, and this module, is to facilitate some common CRUD task with elasticsearch.
 * 
 * <p>it use the {@code RestHighLevelClient} from elastic</p>
 * 
 * <p>
 * For the index name, the prefer way is to use alias, but you cannot create,update, delete on alias with multi index.
 * So you have to pass a {@code Supplier} for index type operation and another for search operation.
 * </p>
 * <p>With {@code Supplier} for index name you are able to have for example index by week,month,year...</p>
 * <p>
 * With elasticsearch you index data in near realtime (1s by default), but you can force to index in realtime.
 * you can do this with the immediate boolean. You can pass it on the constructor, so all operation will be in realtime
 *  (great for testing purpose) or on indexing method.
 * </p>
 * 
 * @author Samuel Nasello
 */
public class ElasticSearchRepository {

	protected final RestHighLevelClient esClient;

	protected final Supplier<String> indexNameIndex;
	
	protected final Supplier<String> indexNameSearch;
	
	protected final String typeName;
	
	protected final boolean immediate;
	
	// === save

	/**
	 * Constructor.
	 * 
	 * @param esClient the {@code RestHighLevelClient}
	 * @param indexNameIndex the {@code Supplier} for indexing operation
	 * @param indexNameSearch the {@code Supplier} for search operation
	 * @param typeName the type name
	 * @param immediate if in realtime or not
	 * @throws NullPointerException if {@code esClient}, {@code indexNameIndex}, 
	 * {@code indexNameSearch} or {@code typeName} is null
	 */
	public ElasticSearchRepository(RestHighLevelClient esClient, Supplier<String> indexNameIndex,
			Supplier<String> indexNameSearch, String typeName, boolean immediate) {
		super();
		
		Objects.requireNonNull(esClient);
		Objects.requireNonNull(indexNameIndex);
		Objects.requireNonNull(indexNameSearch);
		Objects.requireNonNull(indexNameSearch);
		Objects.requireNonNull(typeName);
		
		this.esClient = esClient;
		this.indexNameIndex = indexNameIndex;
		this.indexNameSearch = indexNameSearch;
		this.typeName = typeName;
		this.immediate = immediate;
	}

	/**
	 * Save an object
	 * @param id id of the object, if null then a new object will be created
	 * @param object the object
	 * @param jsonMapper the mapper to convert into json
	 * @param <E> the type of object to save
	 * @return the id
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code object} or {@code jsonMapper} is null
	 */
	public <E> String save(
			final String id,
			final E object,
			final Function<E, String> jsonMapper)
		throws IOException {
		
		Objects.requireNonNull(object);
		Objects.requireNonNull(jsonMapper);
		
		return save(id, object, this.immediate, jsonMapper);
	}

	/**
	 * Save an object
	 * @param id id of the object, if null then a new object will be created
	 * @param object the object
	 * @param immediate define if save in realtime or not
	 * @param jsonMapper the mapper to convert into json
	 * @param <E> the type of object to save
	 * @return the id
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code object} or {@code jsonMapper} is null
	 */
	public <E> String save(
			final String id,
			final E object,
			final boolean immediate,
			final Function<E, String> jsonMapper)
		throws IOException {
		
		Objects.requireNonNull(object);
		Objects.requireNonNull(jsonMapper);
		
		String json = jsonMapper.apply(object);
		IndexResponse indexResponse = executeIndex(id, json, immediate);
		
		return indexResponse.getId();
	}
	
	private IndexResponse executeIndex(
			final String id,
			final String json, 
			final boolean immediate) 
		throws IOException {

		IndexRequest indexRequest = new IndexRequest(this.indexNameIndex.get(), this.typeName, id);
		indexRequest.source(json, XContentType.JSON);
		if (immediate) {
			indexRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return esClient.index(indexRequest);
	}
	
	// === update
	
	/**
	 * Update an object, this is a partial update null value will not be save
	 * @param id id of the object
	 * @param doc the object to update
	 * @param jsonMapper to convert into json
	 * @param <E> the type of object to save
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code id}, {@code doc} or {@code jsonMapper} is null 
	 */
	public <E> void update(
			final String id,
			final E doc,
			final Function<E, String> jsonMapper)
		throws IOException {
		
		Objects.requireNonNull(id);
		Objects.requireNonNull(doc);
		Objects.requireNonNull(jsonMapper);
		
		update(id, doc, this.immediate, jsonMapper);		
	}
	
	/**
	 * Update an object, this is a partial update null value will not be save
	 * @param id id of the object
	 * @param doc the object to update
	 * @param immediate define if update in realtime or not
	 * @param jsonMapper to convert into json
	 * @param <E> the type of object to save
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code id}, {@code doc} or {@code jsonMapper} is null 
	 */
	public <E> void update(
			final String id,
			final E doc,
			final boolean immediate,
			final Function<E, String> jsonMapper)
		throws IOException {
		
		Objects.requireNonNull(id);
		Objects.requireNonNull(doc);
		Objects.requireNonNull(jsonMapper);
		
		executeUpdate(id, jsonMapper.apply(doc), immediate);
	}
	
	private UpdateResponse executeUpdate(
			final String id,
			final String doc,
			final boolean immediate) 
		throws IOException  {
		
		UpdateRequest updateRequest = new UpdateRequest(indexNameIndex.get(), typeName, id);
		updateRequest.doc(doc, XContentType.JSON);
		if(immediate) {
			updateRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return esClient.update(updateRequest);
	}
	
	// === delete
	
	/**
	 * Delete an object
	 * @param id id of the object to delete
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code id} is null 
	 */
	public void delete(
			final String id) 
		throws IOException {
		
		Objects.requireNonNull(id);

		delete(id, this.immediate);
	}
	
	/**
	 * Delete an object
	 * @param id id of the object to delete
	 * @param immediate define if delete in realtime or not
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code id} is null 
	 */
	public void delete(
			final String id,
			final boolean immediate)
		throws IOException {
		
		Objects.requireNonNull(id);
		
		executeDelete(id, immediate);
	}
	
	private DeleteResponse executeDelete(
			final String id,
			final boolean immediate) 
		throws IOException{
		
		DeleteRequest deleteRequest = new DeleteRequest(indexNameIndex.get(), typeName, id);
		if(immediate) {
			deleteRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return esClient.delete(deleteRequest);
	}
	
	// == search request
	
	/**
	 * Find a object by is id, this is a search operation so it works on alias
	 * with multi index.
	 * @param id id of the object to find
	 * @param hitMapper mapper to convert {@code SearchHit} into object
	 * @param <E> the type of object
	 * @return the object if found, or empty
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code id} or {@code hitMapper} is null 
	 */
	public <E> Optional<E> searchById(
			final String id,
			final Function<SearchHit, E> hitMapper) 
		throws IOException{
		
		Objects.requireNonNull(id);
		Objects.requireNonNull(hitMapper);
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.idsQuery(typeName).addIds(id));
		
		SearchResponse response = executeSearchRequest(buildSearchRequest(searchSourceBuilder));
		SearchHits hits = response.getHits();
		if(hits.getHits().length > 0) {
			SearchHit hit = hits.getAt(0);
			return Optional.of(hitMapper.apply(hit));
		}
		return Optional.empty();
	}

	/**
	 * Search with Pagination
	 * @param queryBuilder the elasticsearch query
	 * @param page the pagination information
	 * @param sorts sorts on field, pass a {@code java.util.Collections.emptyList()} if you don't want to sort
	 * @param hitMapper mapper to convert {@code SearchHit} into object
	 * @param <E> the type of object 
	 * @return result into a {@code PageData}
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code queryBuilder}, {@code page}, {@code sorts} or {@code hitMapper} is null 
	 */
	public <E> PageData<E> searchPageData(
			final QueryBuilder queryBuilder,
			final Page page,
			final List<Sort> sorts,
			final Function<SearchHit, E> hitMapper)
		throws IOException{
		
		Objects.requireNonNull(queryBuilder);
		Objects.requireNonNull(page);
		Objects.requireNonNull(sorts);
		Objects.requireNonNull(hitMapper);
		
		SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(queryBuilder);
		addPageInformation(searchSourceBuilder, page);
		addSortInformation(searchSourceBuilder, sorts);
		
		SearchResponse response = executeSearchRequest(buildSearchRequest(searchSourceBuilder));
		List<E> datas = Arrays.stream(response.getHits().getHits())
			.map(hitMapper)
			.collect(Collectors.toList());
		return new PageData<>(page, response.getHits().totalHits, datas);
	}
	
	/**
	 * Search By scrolling data, no pagination !
	 * @param queryBuilder the elasticsearch query
	 * @param sorts sorts on field, pass a {@code java.util.Collections.emptyList()} if you don't want to sort
	 * @param hitMapper mapper to convert {@code SearchHit} into object
	 * @param <E> the type of object to save
	 * @return result into a {@code PageData}
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code queryBuilder}, {@code sorts} or {@code hitMapper} is null 
	 */
	public <E> List<E> searchScroll(
			final QueryBuilder queryBuilder,
			final List<Sort> sorts,
			final Function<SearchHit, E> hitMapper)
		throws IOException{

		Objects.requireNonNull(queryBuilder);
		Objects.requireNonNull(sorts);
		Objects.requireNonNull(hitMapper);
		
		List<E> datas = new ArrayList<>();
		
		SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(queryBuilder);
		addSortInformation(searchSourceBuilder, sorts);
		
		ScrollData<E> scrollDatas = scroll(searchSourceBuilder, hitMapper, new ScrollInfo(30, null));
		datas.addAll(scrollDatas.getDatas());
		while(scrollDatas.hasNext()) {
			scrollDatas = scroll(searchSourceBuilder, hitMapper, scrollDatas.getInfo());
			datas.addAll(scrollDatas.getDatas());
		}
		return datas;
	}
	
	private SearchSourceBuilder buildSearchSourceBuilder(
			final QueryBuilder queryBuilder) {
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.query(queryBuilder);		
		return searchSourceBuilder;
	}
	
	private void addPageInformation(
			final SearchSourceBuilder searchSourceBuilder,
			final Page page) {
		
		searchSourceBuilder.size(page.getLimit()).from(page.getOffset());
	}
	
	private void addSortInformation(
			final SearchSourceBuilder searchSourceBuilder,
			final List<Sort> sorts) {
		
		sorts.forEach(s -> searchSourceBuilder.sort(s.getField(), toSortOrder(s.getDirection())));
	}
	
	private <E> ScrollData<E> scroll(
			final SearchSourceBuilder searchSourceBuilder,
			final Function<SearchHit, E> hitMapper,
			final ScrollInfo scrollInfo)
		throws IOException{
		
		final Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scrollInfo.getKeepAliveSecond()));
		
		final SearchResponse searchResponse;
		if(scrollInfo.getScrollId() != null) {
			SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollInfo.getScrollId());
			searchScrollRequest.scroll(scroll);
			searchResponse = esClient.searchScroll(searchScrollRequest);
		}else {
			searchResponse = executeSearchRequest(
					addScrollInformation(
							buildSearchRequest(searchSourceBuilder),
							scroll));
		}
		
		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();

		if(searchHits.length > 0) {
			return new ScrollData<>(
					scrollInfo.withScrollId(searchResponse.getScrollId()),
					toDatas(searchHits, hitMapper)
			);
		}else {
			clearScroll(scrollId);
			return new ScrollData<>(
				scrollInfo.withScrollId(searchResponse.getScrollId()),
				Collections.emptyList()
			);
		}	

	}

	private boolean clearScroll(
			final String scrollId) 
		throws IOException {
		
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse response = esClient.clearScroll(clearScrollRequest);
		return response.isSucceeded();
	}
	
	private SearchRequest buildSearchRequest(
			SearchSourceBuilder searchSourceBuilder) {
		
		SearchRequest searchRequest = new SearchRequest(indexNameIndex.get()).types(typeName);
		searchRequest.source(searchSourceBuilder);
		return searchRequest;
	}
	
	private SearchResponse executeSearchRequest(
			SearchRequest searchRequest)
		throws IOException {
		
		return esClient.search(searchRequest);
	}
	
	private SearchRequest addScrollInformation(
			final SearchRequest searchRequest,
			final Scroll scroll) {
		
		searchRequest.scroll(scroll);
		return searchRequest;
	}
	
	// == bulk

	/**
	 * Save object with a bulk request.
	 * @param objects objects to save
	 * @param jsonMapper mapper to convert object into json
	 * @param <E> the type of object to save, must be an {@code Identifiable}
	 * @return the bulkResponse if the bulk was execute (if list have element)
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code objects} or {@code jsonMapper} is null 
	 */
    public <E extends Identifiable> Optional<BulkResponse> bulkSave(
    		final java.util.List<E> objects,
    		final Function<E, String> jsonMapper)
    	throws IOException {

		Objects.requireNonNull(objects);
		Objects.requireNonNull(jsonMapper);
		
    	if(objects.isEmpty()) {
    		return Optional.empty();
    	}
    	BulkRequest bulkRequest = new BulkRequest();
        for(E obj : objects) {
        	IndexRequest indexRequest = new IndexRequest(indexNameIndex.get(), typeName, obj.getId())  
        			.source(jsonMapper.apply(obj), XContentType.JSON);
            bulkRequest.add(
            		indexRequest
            );
        }
        
        return executeBulk(bulkRequest);
    }
    
    /**
     * Execute a bulk request.
     * @param bulkRequest The bulk request
 	 * @return the bulkResponse if the bulk was execute (if list have element)
     * @throws IOException io probleme with elasticsearch
	 * @throws NullPointerException if {@code bulkRequest} is null 
     */
    public Optional<BulkResponse> executeBulk(
    		BulkRequest bulkRequest) 
    	throws IOException {
    	
		Objects.requireNonNull(bulkRequest);
		
        if(bulkRequest.numberOfActions() > 0 ) {
	        return Optional.of(esClient.bulk(bulkRequest));
        }
        return Optional.empty();
    }
    
    /**
     * Helper method to log failure
     * @param bulkResponse the bulkresponse
     * @param logConsumer the log consumer
	 * @throws NullPointerException if {@code bulkResponse} or {@code logConsumer} is null 
     */
    public void bulkLogFailure(
    		final BulkResponse bulkResponse,
    		final Consumer<String> logConsumer) {
    	
		Objects.requireNonNull(bulkResponse);
		Objects.requireNonNull(logConsumer);
		
		if(bulkResponse.hasFailures()) {
			bulkLogFailureItems(bulkResponse.getItems(), logConsumer);
		}
    }
    
    private void bulkLogFailureItems(
    		final BulkItemResponse[] responses,
    		final Consumer<String> logConsumer) {
    	
        for (BulkItemResponse response : responses) {
            if (response.isFailed()) {
            	logConsumer.accept(response.getFailureMessage());
            }
        }
    }
    
    // == utils functions
    
	private SortOrder toSortOrder(
			final SortDirection sd) {
		
		switch (sd) {
		case ASC:
			return SortOrder.ASC;
		case DSC:
			return SortOrder.DESC;
		default:
			throw new IllegalArgumentException("unknown SortDirection : " + sd);
		}
	}
	
	// == transform datas
	
	private <E> List<E> toDatas(
			final SearchHit[] hits,
			final Function<SearchHit, E> hitMapper){
		
		List<E> datas = new ArrayList<>();
		appendDatas(hits, datas, hitMapper);
		return datas;
	}
	
	private <E> void appendDatas(
			final SearchHit[] hits,
			final List<E> datasToAppend,
			final Function<SearchHit, E> hitMapper){
		
		for (int i = 0; i < hits.length; i++) {
			E data = hitMapper.apply(hits[i]);
			datasToAppend.add(data);
		}
	}
}
