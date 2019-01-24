package fr.snasello.datas.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PageTest {

	@Test
	public void testNew() {
		Page page = new Page(3, 32);
		assertEquals(32, page.getLimit());
		assertEquals(3, page.getOffset());
	}
	
	@Test
	public void testFirstPage() {
		Page page = Page.firstPageWithLimit(10);
		assertEquals(10, page.getLimit());
		assertEquals(Page.FIRST_OFFSET, page.getOffset());
	}
	
	@Test
	public void testNextPage() {
		Page page = new Page(0, 10);
		Page nextPage = page.nextPage();
		assertEquals(10, nextPage.getOffset());
		assertEquals(10, nextPage.getLimit());
	}
}
