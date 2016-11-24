package topology;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

public class Test {
	 public Test(Properties props){
		  System.out.println("construct");
		 }
		 public String build(){
		  System.out.println("build");
		  return "a";
		 }
		 
		 public static void main(String []args) throws Exception{
		  Properties props = new Properties();
		  Class<?> clazz = Class.forName("topology.Test");
		  Constructor<?> constructor = clazz.getConstructor(Properties.class);
		  Method mothod = clazz.getMethod("build", null);
		  Object obj = constructor.newInstance(props);
		  String result = (String)mothod.invoke(obj, null);
		  System.out.println(result);
		  
		   String str = "str";
		   System.out.println(str=""!= null ? str:"2");
		  
		 }
}
