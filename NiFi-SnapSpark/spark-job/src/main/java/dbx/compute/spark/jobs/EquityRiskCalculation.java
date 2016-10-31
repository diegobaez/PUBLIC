package dbx.compute.spark.jobs;

public class EquityRiskCalculation {
	public static void main(String[] args) throws InterruptedException {
		
		String[] data = args[0].split("\\|");

		int num_args = data.length;
		int charnum = 0;
		for (int i=0;i<data.length;i++){
			charnum += data[i].length();
			Thread.sleep(1000);
		}
		System.out.println("Spark-Processed|"+num_args+"|"+charnum);
		
		for(int i=0;i<num_args;i++){
			System.out.print(i+"|");
			for(int j=0;j<charnum;j++){
				System.out.print(j+"|");
			}
		System.out.println();
		}
	}
}
