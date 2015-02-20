//test

package org.krisna.map;

class HashMapDemo{
	
 public static void main(String args[]){
	 String line = "388222545,00555057251414740538,Products, Services and Fund Transfer Menu,2014-10-31 13:00:40.870000000";
	 line=line.replace(",Products, Services and Fund Transfer Menu","Products and Services menu");
	 System.out.println(line);
/* 
  Map<Integer,String> hm=new TreeMap<Integer,String>();

  hm.put(100,"Amit");
  hm.put(102,"Vijay");
  hm.put(101,"Rahul");
  for(short i=0;i<5;i++)
  {
  try{
      String data=null;
      System.out.println(data.length());
   }catch(Exception e){System.out.println(e);}
   System.out.println("rest of the code...");

  for(Map.Entry<Integer,String> m:hm.entrySet()){
   System.out.println(m.getKey()+" "+m.getValue());
  }
  
  }*/
 }
}

