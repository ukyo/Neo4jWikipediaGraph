package wikipediagraph.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class MySqlDumpReader implements Iterable<String[]>{
	private BufferedReader reader;
	private String[] line = null;
	private int index = 0;
	
	public MySqlDumpReader(String filename) {
		this(filename, "UTF-8");
	}
	
	public MySqlDumpReader(String filename, String encoding) {
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), encoding));
			String s = null;
			while((s = reader.readLine()) != null) {
				if (s.matches(".*ALTER TABLE.*")) break;
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class SqlReaderIterator implements Iterator<String[]> {

		@Override
		public boolean hasNext() {
			try {
				if (line != null && index < line.length) {
					return true;
				} else {
					String s = reader.readLine();
					if (s.matches(".*ALTER TABLE.*")) {
						reader.close();
						return false;
					}
					
					line = s.split(" VALUES [(]")[1].split("[)];")[0].split("[)],[(]");
					index = 0;
					return true;
				}
			} catch (IOException e) { }
			return false;
		}

		@Override
		public String[] next() {
			return line[index++].split(",");
		}
		
		@Override
		public void remove() {	}
		
	}
	
	@Override
	public Iterator<String[]> iterator() {
		return new SqlReaderIterator();
	}
	
	public static void main(String[] args) {
		for(String[] s: new MySqlDumpReader("D:\\jawiki-latest-pagelinks.sql")) {
			System.out.println(s[0]+" "+s[1]+" "+s[2]);
		}
	}
}
