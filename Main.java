import java.io.*;

public class Main{
	public static void main(String... args){
		if(args.length != 2){
			System.out.println("Usage:");
			System.out.println("<csv file> <n>");
			System.out.println("csv file  File to split");
			System.out.println("n         Number of files to split into");
			System.out.println();
			System.out.println("This program will split the input file into n output files.");
			System.out.println("The output files are named as <input name>.{1..n}.csv.");
			System.exit(1);
		}
		
		int n = 1;
		try{
			n = Integer.parseInt(args[1]);
		}
		catch(NumberFormatException e){
			System.err.println("Error! n must be an integer, is: " + args[1]);
			System.exit(2);
		}
		if(n <= 0){
			System.err.println("Error! n must be greater than 0.");
			System.exit(2);
		}
		
		File input = new File(args[0]);
		CsvSplitter s = new CsvSplitter(input, n);
		try{
			s.splitIntoFiles();
			s.executor.shutdown();
		}
		catch(FileNotFoundException e){
			System.err.printf("Error: file '%s' does not exist\n", input.toString());
			System.exit(3);
		}
		catch(IOException e){
			if(!input.isFile())
				System.err.printf("Error: '%s' is not a file\n", input.toString());
			else
				System.err.println("Error: " + e.getMessage());
			System.exit(3);
		}
	}
}