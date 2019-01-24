package fr.snasello.datas.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class ScrollDataTest {

	@Test
	public void testNew() {
		ScrollData<String> scrollData = new ScrollData<>(new ScrollInfo(103, "123456"), Arrays.asList("azerty", "qwerty"));
		assertEquals(Arrays.asList("azerty","qwerty"), scrollData.getDatas());
		assertTrue(scrollData.hasNext());
	}
	
	@Test
	public void testEmpty() {
		ScrollData<String> scrollData = new ScrollData<>(new ScrollInfo(103, "123456"), new ArrayList<>());
		assertFalse(scrollData.hasNext());
	}
}
