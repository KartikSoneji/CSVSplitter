import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.csv.*;

public class CsvSplitter{
	private File input;
	private int n;
	
	private CSVRecord headers;
	private List<CSVRecord> records;
	
	public ExecutorService executor;
	
	public CsvSplitter(File input, int n){
		this.input = input;
		this.n = n;
	}
	
	public void splitIntoFiles() throws IOException{
		loadFile();
		startThreads();
	}
	
	public void loadFile() throws IOException{
		CSVParser parser = CSVParser.parse(input, java.nio.charset.StandardCharsets.UTF_8, CSVFormat.DEFAULT);
		records = parser.getRecords();
		if(records.size() > 0)
			headers = records.remove(0);
		parser.close();
	}
	
	public void startThreads(){
		if(records == null)
			throw new IllegalStateException("File not loaded! loadFile() must be called before");
		
		executor = Executors.newFixedThreadPool(8);
		
		String baseName = input.getName();
		if(baseName.endsWith(".csv"))
			baseName = baseName.substring(0, baseName.length() - 3);
		
		int rows = records.size()/n;
		if(rows == 0)
			rows = 1;
		for(int i = 0, from, to; i < n; i++){
			from = i * rows;
			to = from + rows;
			if(i == n - 1)
				to = records.size();
			
			executor.submit(new ChunkWriter(from, to, baseName + (i + 1) + ".csv"));	
		}
	}
	
	private class ChunkWriter implements Runnable{
		int from, to;
		String outputName;
		
		public ChunkWriter(int from, int to, String outputName){			
			this.from = from;
			this.to = to;
			this.outputName = outputName;
		}
		
		public void run(){
			try{
				CSVPrinter printer = new CSVPrinter(new FileWriter(outputName), CSVFormat.EXCEL);
				printer.printRecord(headers);
				for(int i = from; i < to && i < records.size(); i++)
					printer.printRecord(records.get(i));
				printer.close(true);
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}