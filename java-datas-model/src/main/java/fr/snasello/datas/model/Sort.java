package fr.snasello.datas.model;

import lombok.Value;

@Value
public class Sort {

	private final String field;
	
	private final SortDirection direction;
}
