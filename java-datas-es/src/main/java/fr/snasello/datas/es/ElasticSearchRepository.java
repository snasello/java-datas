package fr.snasello.datas.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ElasticSearchRepository {

	protected final RestHighLevelClient esClient;

	protected final Supplier<String> indexNameIndex;
	
	protected final Supplier<String> indexNameSearch;
	
	protected final String typeName;
	
	protected final boolean immediate;
	
	// === save

	public <E> String save(
			final String id,
			@NonNull final E object,
			@NonNull final Function<E, String> jsonMapper)
		throws IOException {
		
		return save(id, object, this.immediate, jsonMapper);
	}

	public <E> String save(
			final String id,
			@NonNull final E object,
			final boolean immediate,
			@NonNull final Function<E, String> jsonMapper)
		throws IOException {
		
		log.debug("save({},{})", object, immediate);
		
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
	
	public <E> void update(
			@NonNull final String id,
			@NonNull final E doc,
			@NonNull final Function<E, String> jsonMapper)
		throws IOException {
		
		update(id, doc, this.immediate, jsonMapper);		
	}
	
	public <E> void update(
			@NonNull final String id,
			@NonNull final E doc,
			final boolean immediate,
			@NonNull final Function<E, String> jsonMapper)
		throws IOException {
		
		log.debug("update({},{})", doc, immediate);
		
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
	
	public void delete(
			@NonNull final String id) 
		throws IOException {
		
		delete(id, this.immediate);
	}
	
	public void delete(
			@NonNull final String id,
			final boolean immediate)
		throws IOException {
		
		log.debug("delete({},{})", id, immediate);
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
	
	public <E> Optional<E> searchById(
			@NonNull final String id,
			@NonNull final Function<SearchHit, E> hitMapper) 
		throws IOException{
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.idsQuery(typeName).addIds(id));
		
		SearchResponse response = executeSearch(searchSourceBuilder);
		SearchHits hits = response.getHits();
		if(hits.getHits().length > 0) {
			SearchHit hit = hits.getAt(0);
			return Optional.of(hitMapper.apply(hit));
		}
		return Optional.empty();
	}

	public <E> PageData<E> searchPageData(
			@NonNull final QueryBuilder queryBuilder,
			@NonNull final Page page,
			@NonNull final List<Sort> sorts,
			@NonNull final Function<SearchHit, E> hitMapper)
		throws IOException{
		
		Optional<Page> pageOpt = Optional.of(page);
		
		SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(queryBuilder, pageOpt, sorts);
		SearchResponse response =  executeSearch(searchSourceBuilder);
		List<E> datas = Arrays.stream(response.getHits().getHits())
			.map(hitMapper)
			.collect(Collectors.toList());
		return new PageData<>(pageOpt, response.getHits().totalHits, datas);
	}
	
	public <E> List<E> searchScroll(
			@NonNull final QueryBuilder queryBuilder,
			@NonNull final List<Sort> sorts,
			@NonNull final Function<SearchHit, E> hitMapper)
		throws IOException{

		List<E> datas = new ArrayList<>();
		
		SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder(queryBuilder, Optional.empty(), sorts);
		
		ScrollData<E> scrollDatas = scroll(searchSourceBuilder, hitMapper, new ScrollInfo(30, null));
		datas.addAll(scrollDatas.getDatas());
		while(scrollDatas.hasNext()) {
			scrollDatas = scroll(searchSourceBuilder, hitMapper, scrollDatas.getInfo());
			datas.addAll(scrollDatas.getDatas());
		}
		return datas;
	}
	
	private SearchSourceBuilder buildSearchSourceBuilder(
			final QueryBuilder queryBuilder,
			final Optional<Page> page,
			final List<Sort> sorts) {
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.query(queryBuilder);
		
		page.ifPresent(p -> searchSourceBuilder.size(p.getLimit()).from(p.getOffset()));
		sorts.forEach(s -> searchSourceBuilder.sort(s.getField(), toSortOrder(s.getDirection())));
		
		return searchSourceBuilder;
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
			searchResponse = executeSearch(searchSourceBuilder, Optional.of(scroll));
		}
		
		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();

		if(searchHits != null && searchHits.length > 0) {
			return new ScrollData<>(
					scrollInfo.withScrollId(searchResponse.getScrollId()),
					toDatas(searchHits, hitMapper)
			);
		}else {
			if(!clearScroll(scrollId)) {
				log.error("unable to clear scroll with id {}", scrollId);
			}
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

	private SearchResponse executeSearch(
			SearchSourceBuilder searchSourceBuilder)
		throws IOException {
		
		return executeSearch(searchSourceBuilder, Optional.empty());
	}
	
	private SearchResponse executeSearch(
			SearchSourceBuilder searchSourceBuilder,
			Optional<Scroll> scroll)
		throws IOException {
		
		SearchRequest searchRequest = new SearchRequest(indexNameIndex.get()).types(typeName);
		searchRequest.source(searchSourceBuilder);
		scroll.ifPresent(searchRequest::scroll);
		return esClient.search(searchRequest);
	}
	
	// == bulk

    public <E extends Identifiable> Optional<BulkResponse> bulkSave(
    		@NonNull final java.util.List<E> objects,
    		@NonNull final Function<E, String> jsonMapper)
    	throws IOException {

    	log.debug("bulkSave {} objects", objects.size());
    	if(objects.isEmpty()) {
    		return Optional.empty();
    	}
    	BulkRequest bulkRequest = new BulkRequest();
        for(E obj : objects) {
            try {
            	IndexRequest indexRequest = new IndexRequest(indexNameIndex.get(), typeName, obj.getId())  
            			.source(jsonMapper.apply(obj), XContentType.JSON);
                bulkRequest.add(
                		indexRequest
                );
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        
        return executeBulk(bulkRequest);
    }
    
    public Optional<BulkResponse> executeBulk(
    		@NonNull BulkRequest bulkRequest) 
    	throws IOException {
    	
        if(bulkRequest.numberOfActions() > 0 ) {
	        BulkResponse bulkResponse = esClient.bulk(bulkRequest);
	        if (bulkResponse.hasFailures()) {
	            for (BulkItemResponse response : bulkResponse.getItems()) {
	                if (response.isFailed()) {
	                    log.error(response.getFailureMessage());
	                }
	            }
	        }
	        return Optional.of(bulkResponse);
        }
        return Optional.empty();
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
