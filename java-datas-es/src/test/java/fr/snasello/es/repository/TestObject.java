package fr.snasello.es.repository;

import fr.snasello.datas.es.Identifiable;
import lombok.Data;

@Data
public class TestObject implements Identifiable{

	private String id;
	
	private String name;
	
}
