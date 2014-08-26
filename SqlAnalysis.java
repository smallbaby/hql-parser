package smallbaby.io.cole.zhang;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

/**
 * SQL字段级分析,思路有了。待完善代码
 * @author kaizhang
 *
 */
public class SqlAnalysis {
	
	public static void main(String[] args) throws Exception {
		String query = "select id, name from log_info"; 
		query="select a.id,b.name,c.addr,d.pwd,f.tel from a join b join c join d join f";
		run(query);
		
	}
	
	
	public static void run(String query) throws Exception {
		ParseDriver pd = new ParseDriver();
		ASTNode tree = pd.parse(query);
		System.out.println("************开始分析*********************\n");
		analysis((ASTNode)tree.getChild(0));
		System.out.println("\n************END*********************");
	}
	
	
	/**
	 * 分析入口
	 * 一个query分为2部分， INSERT 和 FROM   
	 * 1. insert：不写create 和 insert into的默认插入到TMP_FILE，select部分包含在insert结构中
	 * 2. from : 后边可能为子查询、join。单表、union
	 * @param ast
	 */
	 public static void analysis(ASTNode ast) {
		 int len  = ast.getChildCount();
		 if(len > 0) {
			 for (Node n : ast.getChildren()) {
			        ASTNode asn = (ASTNode)n;
			        switch (asn.getToken().getType()) {//根据类型分发
			        case HiveParser.TOK_FROM:
			            ASTNode ASTNodetmp02 = (ASTNode)asn.getChild(0);
			            fromAnalysis(ASTNodetmp02);
			            break;     
			        case HiveParser.TOK_INSERT: // 分两部分，insert into and select cols
			        	//
			        	for (int i = 0; i < asn.getChildCount(); i++) {
			        		insertAnalysis((ASTNode)asn.getChild(i));							
						}
			        	break;
			        case HiveParser.TOK_UNION: 
			            int childcount = asn.getChildCount();
			            for (int childpos = 0; childpos < childcount; ++childpos) {    
			              analysis((ASTNode)asn.getChild(childpos));
			            }
			            break;  
			        }
			      }
		 } else {
			 System.out.println(ast.getText());
		 }
	 }
	 
	 /**
	  * 子查询分析
	  * @param subQuery
	  */
	 public static void subQueryAnalysis(ASTNode subQuery) {
		    
		    int cc = 0;
		    int cp = 0;
		    
		    switch (subQuery.getToken().getType()) {
		    case HiveParser.TOK_QUERY:
		      cc = subQuery.getChildCount();
		      
		      for ( cp = 0; cp < cc; ++cp) {
		        ASTNode atmp = (ASTNode)subQuery.getChild(cp);
		                
		        switch (atmp.getToken().getType()) {    
		        case HiveParser.TOK_FROM:
		          fromAnalysis( (ASTNode)atmp.getChild(0));
		          break;                                           
		        }  
		      }
		      break;
		    case HiveParser.TOK_UNION: 
		      cc = subQuery.getChildCount();
		      
		      for ( cp = 0; cp < cc; ++cp) {    
		    	  subQueryAnalysis( (ASTNode)subQuery.getChild(cp));
		      }
		      break;  
		    }  
		  }  
	 
	 /**
	  * insert部分分析
	  * 1. TOK_DESTINATION 为目标表，不限定时，默认为tok_dir/tmp_file
	  * 2. TOK_SELECT 为 select部分
	  * @param ast
	  */
	 public static void insertAnalysis(ASTNode ast) {
		 switch(ast.getToken().getType()) {
		 case HiveParser.TOK_DESTINATION:
			 if(ast.getChild(0).getType() == 616)  {// tok dir
				 System.out.println("* Insert Table:\t" + ast.getChild(0).getChild(0).getText());
			 } else {
				 System.out.println("* Insert Table:\t" + ast.getChild(0).getChild(0).getChild(0).getText());
			 }
			 break;
		 case HiveParser.TOK_SELECT:
			 int len = ast.getChildCount();
			 for (int i = 0; i < len; i++) {
				 ASTNode astmp = (ASTNode)ast.getChild(i);
				 if(astmp.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
					 System.out.println("* Select Cols :\t" + astmp.getChild(0).getChild(0).getText());
				 } else if(astmp.getChild(0).getType() == HiveParser.TOK_ALLCOLREF){ // select *
					 
					 String co = null;
					 try {
						 if(astmp.getChild(0).getChild(0).getType() == HiveParser.TOK_TABNAME) {
							 co = astmp.getChild(0).getChild(0).getChild(0).getText() + ".*";
						 } else {
							 co = astmp.getChild(0).getChild(0).getChild(0).getText();
						 }
					 }catch(Exception e) {
						 co = "*";
					 }
					 System.out.println("* Select Cols :\t" + co);
				 } else {
					 System.out.println("* Select Cols :\t" + astmp.getChild(0).getChild(0).getChild(0).getText() + "." + astmp.getChild(0).getChild(1).getText());
				 }
			}
			 break;
		 }
	 }
	 
	 /**
	  * from部分分析
	  * 1. TOK_TABREF 表
	  * 2. *_JOIN 各种join
	  * 3. subQuery：子查询
	  * @param qf
	  */
	 private static void fromAnalysis(ASTNode qf) {
		// TODO Auto-generated method stub
			    
			    int cc = 0;
			    int cp = 0;
			    
			    switch (qf.getToken().getType()) {    
			    case HiveParser.TOK_TABREF:
			      ASTNode atmp = (ASTNode)qf.getChild(0);
			       String tb = atmp.getChildCount()==1 ? atmp.getChild(0).toString()  : atmp.getChild(0).toString()  + "." + atmp.getChild(1).toString() ;
			      String res = qf.getChildCount()==1 ? 
			    		  tb : tb + "\talias : \t" + qf.getChild(1).toString();
			      System.out.println("* From Table :\t" + res);      
				     
			      break;  
			    case HiveParser.TOK_LEFTOUTERJOIN:
			    	cc = qf.getChildCount();
				      
				      for ( cp = 0; cp < cc; cp++) {    
				        ASTNode atm = (ASTNode)qf.getChild(cp);
				        fromAnalysis(atm);
				      }
				      break;  
			    case HiveParser.TOK_JOIN:
			      cc = qf.getChildCount();
			      
			      for ( cp = 0; cp < cc; cp++) {    
			        ASTNode atm = (ASTNode)qf.getChild(cp);
			        fromAnalysis(atm);
			      }
			      break;  
			    case HiveParser.TOK_SUBQUERY: // 子查询
			    	// 看count，
			    	// 只要子查询，count=2, 一个子查询[可能为嵌套]，一个为别名
			    	//分析第一个：
			    	//书写形式有：1.  TOK_QUERY 2. TOK_UNION 3.TOK_JOIN
			     // SubParseQuery(ASTNodeParseQuery,ASTNodetmp03);
			    	subQueryAnalysis((ASTNode)qf.getChild(0));
			      break;   
			    case HiveParser.TOK_LATERAL_VIEW:
			      cc = qf.getChildCount();
			      for ( cp = 0; cp < cc; ++cp) {    
			    	  fromAnalysis((ASTNode)qf.getChild(cp));
			      }
			      break;                                                          
			    }    
	}
