import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.csv.*;

public class CsvSplitter{
	private File input;
	private int n;
	
	private List<String> headers;
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
		CSVParser parser = CSVParser.parse(
			input,
			java.nio.charset.StandardCharsets.UTF_8,
			CSVFormat.DEFAULT.withFirstRecordAsHeader()
		);
		records = parser.getRecords();
		if(records.size() > 0)
			headers = parser.getHeaderNames();
		parser.close();
	}
	
	public void startThreads(){
		if(records == null)
			throw new IllegalStateException("File not loaded! loadFile() must be called before");
		
		executor = Executors.newFixedThreadPool(8);
		
		String baseName = input.getName();
		if(baseName.endsWith(".csv"))
			baseName = baseName.substring(0, baseName.length() - 4);
		
		int rows = records.size()/n;
		if(rows == 0)
			rows = 1;
		for(int i = 0, from, to; i < n; i++){
			from = i * rows;
			to = from + rows;
			if(i == n - 1)
				to = records.size();
			
			executor.submit(new ChunkWriter(
				headers,
				records.subList(from, to),
				new File(input, baseName + "." + (i + 1) + ".csv"))
			);
		}
	}	
}

class ChunkWriter implements Runnable{
	List<String> headers;
	List<CSVRecord> records;
	File output;
	
	public ChunkWriter(List<String> headers, List<CSVRecord> records, File output){
		this.headers = headers;
		this.records = records;
		this.output = output;
	}
	
	public void run(){
		try(
			CSVPrinter printer = new CSVPrinter(new FileWriter(output), CSVFormat.EXCEL)
		){
			printer.printRecord(headers);
			for(CSVRecord record:records)
				printer.printRecord(record);
			printer.close(true);
		}
		catch(IOException e){
			System.err.printf("Error: could not writing to file '%s'\n", output);
			System.out.println(e.getMessage().indent(4));
		}
	}
}