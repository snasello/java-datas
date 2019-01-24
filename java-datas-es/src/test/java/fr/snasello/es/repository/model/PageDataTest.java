package fr.snasello.es.repository.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class PageDataTest {

	@Test
	public void testNew() {		
		PageData<String> pageData = new PageData<>(Optional.of(new Page(3, 43)), 25, Arrays.asList("azerty","qwerty"));
		
		assertEquals(25, pageData.getTotalSize());
		
		Page page = pageData.getPage().get();
		assertEquals(43, page.getLimit());
		assertEquals(3, page.getOffset());
		
		assertEquals(Arrays.asList("azerty","qwerty"), pageData.getDatas());
	}
	
	@Test
	public void testEmpty() {
		PageData<String> pageData = PageData.empty(Optional.of(new Page(4, 32)));
		
		assertEquals(0, pageData.getTotalSize());
		
		Page page = pageData.getPage().get();
		assertEquals(32, page.getLimit());
		assertEquals(4, page.getOffset());
		
		assertTrue(pageData.getDatas().isEmpty());
	}
	
	@Test
	public void testMap() {
		PageData<String> pageData = new PageData<>(Optional.of(new Page(3, 43)), 25, Arrays.asList("azerty","qwerty"));
		
		PageData<String> pageDataMapped = pageData.mapDatas(value -> value + "UPD");
		
		assertEquals(Arrays.asList("azerty","qwerty"), pageData.getDatas());
		assertEquals(Arrays.asList("azertyUPD","qwertyUPD"), pageDataMapped.getDatas());
		
	}
}
