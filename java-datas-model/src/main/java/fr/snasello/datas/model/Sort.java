package fr.snasello.datas.model;

import java.util.Objects;

public class Sort {

	private final String field;
	
	private final SortDirection direction;

	public Sort(String field, SortDirection direction) {
		super();
		
		Objects.requireNonNull(field);
		Objects.requireNonNull(direction);
		
		this.field = field;
		this.direction = direction;
	}

	public String getField() {
		return field;
	}

	public SortDirection getDirection() {
		return direction;
	}
	
}
