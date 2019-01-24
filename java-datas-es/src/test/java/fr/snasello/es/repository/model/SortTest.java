package fr.snasello.es.repository.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SortTest {

	@Test
	public void testNewAsc() {
		Sort sort = new Sort("azerty", SortDirection.ASC);
		assertEquals("azerty", sort.getField());
		assertEquals(SortDirection.ASC, sort.getDirection());
	}
	
	@Test
	public void testNewDesc() {
		Sort sort = new Sort("azerty2", SortDirection.DSC);
		assertEquals("azerty2", sort.getField());
		assertEquals(SortDirection.DSC, sort.getDirection());
	}
}
