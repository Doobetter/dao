package org.roc.topology.cmd;


public class CmdTest {
	public static void main(String[] args) {
		
		Cmd cmd=new Cmd("UserEachAttention");
		cmd.addParam("date", "日期");
		cmd.addParam("input_path", "mds_user_fanslist列表的HDFS路径");	
		cmd.addParam("output_path", "HDFS上输出目录");	
		cmd.addParam("reduce-num", "Reduce个数，数值");
		cmd.addParam("add_1", "test_add");
		
		String[] Args = new String[]{"--date=20120214","--input_path=/result/wenbo3/temp/fanslist","--add_1=1","--output_path=/result/wenbo3/temp/eachfollow","--reduce-num=300"};
		cmd.parse(Args);	
		
		cmd.printHelp();
		String fanslist_path=cmd.getArgValue("date");
		String input_path=cmd.getArgValue("input_path");
		String output_path=cmd.getArgValue("output_path");
		String reduce_num=cmd.getArgValue("reduce-num");
		
		
	}

}
