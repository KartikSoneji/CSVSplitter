import java.io.*;

public class Main{
	public static void main(String... args) throws IOException{
		if(args.length != 2){
			System.out.println("Usage:");
			System.out.println();
			System.out.println("<csv file> <n>");
			System.out.println("csv file\tFile to split");
			System.out.println("n\t\tNumber of files to split into");
			System.out.println();
			System.out.println("This program will split the input file into n output files.");
			System.out.println("The output files are named as <input name>.{1..n}.csv.");
			return;
		}
		
		File input = new File(args[0]);
		if(!input.exists()){
			System.out.println("Error! file does not exist: " + args[0]);
			return;
		}
		else if(!input.isFile()){
			System.out.println("Error! " + args[0] + " is not a file");
			return;
		}
		
		int n = -1;
		try{
			n = Integer.parseInt(args[1]);
		}
		catch(NumberFormatException e){
			System.err.println("Error! n must be an integer, is: " + args[1]);
			return;
		}
		if(n <= 0){
			System.out.println("Error! n must be greater than 0.");
			return;
		}
		
		CsvSplitter s = new CsvSplitter(input, n);
		s.splitIntoFiles();
		s.executor.shutdown();
	}
}