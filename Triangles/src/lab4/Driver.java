package lab4;

public class Driver {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=3){
			System.err.println("please input inputpath,midresultpath,outputpath");
			System.exit(-1);
		}
		
		String para1[] ={args[0],args[1]};
		stage1.main(para1);
		String para2[] ={args[1],args[2]};
		stage2.main(para2);
	}

}
