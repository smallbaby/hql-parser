package com.jd.sql.p;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcUtils;

/**
 *
 * 借助阿里开源的druid项目代码
 * SQL 执行分解动作....
 * 1. 不支持的操作：lateral VIEW 
 * 2. 不支持insert xxxx select *  
 * 3. 不支持create xxxx select *
 * 4. 偶尔bui支持case函数，待测试
 * 5. 只支持查询语句 SQLSelectStatement
 * TODO: 2、3特殊处理下   SUCC
 * @author zhangkai3
 *
 */
public class SqlExplain {
	
	public static String pPrefix = null;
	
	public static void main(String[] args) throws InterruptedException {
		//yes String sql = " SELECT    CASE WHEN m.cata_id IS NULL THEN n.cata_id ELSE m.cata_id END AS cata_id,   n.att_id,   n.att_name,   b.value_id,   b.value_name,   n.type FROM  (SELECT att_id,att_name,cata_id,group_id,type FROM fdm.fdm_forest_attribute_chain WHERE dp='ACTIVE') n  LEFT OUTER JOIN (SELECT cata_id,group_id ,type FROM fdm.fdm_forest_attribute_group_chain WHERE dp='ACTIVE') m ON n.group_id=m.group_id AND n.type=m.type  LEFT OUTER JOIN (SELECT att_id,value_id, value_name,type FROM fdm.fdm_forest_attribute_value_chain WHERE dp='ACTIVE') b  ON n.att_id=b.att_id AND n.type=b.type where a=1; ";
		// no String sql = "select * from ( select county.county_id         as dim_county_id,         county.county_name      as dim_county_name,         county.county_id        as county_id,         city.city_id            as dim_city_id,         city.city_name          as dim_city_name,         city.city_id            as city_id,         province.province_id    as dim_province_id,         province.province_name  as dim_province_name,         province.province_id    as province_id,         case when province.province_id in (2,12,14,15) then '3'             when province.province_id in (4,22,24,25,26) then '4'             when province.province_id in (1,12,6,3,13,5,11) then '6'             when province.province_id in (23,16,19,20) then '10'             when province.province_id in (17,18,7,21) then '600'             when province.province_id in (8,9,10) then '611'             when province.province_id in (27,28,29,30,31) then '645'         end                  as dim_subd_num,         case when province.province_id in (2,12,14,15) then '上海分公司'             when province.province_id in (4,22,24,25,26) then '成都分公司'             when province.province_id in (1,12,6,3,13,5,11) then '北京分公司'             when province.province_id in (23,16,19,20) then '广州分公司'             when province.province_id in (17,18,7,21) then '武汉分公司'             when province.province_id in (8,9,10) then '沈阳分公司'             when province.province_id in (27,28,29,30,31) then '西安分公司'         end                 as dim_subd_name,         case when province.province_id in (2,12,14,15) then '3'             when province.province_id in (4,22,24,25,26) then '4'             when province.province_id in (1,12,6,3,13,5,11) then '6'             when province.province_id in (23,16,19,20) then '10'             when province.province_id in (17,18,7,21) then '600'             when province.province_id in (8,9,10) then '611'             when province.province_id in (27,28,29,30,31) then '645'         end                  as subd_num,         '' as sort_num  from         (select loc_id as city_id, loc_name as city_name, super_loc_id as province_id from tmp.tmp_assort where loc_type = 'city' ) city      left outer join         (select loc_id as county_id, loc_name as county_name, super_loc_id as city_id from tmp.tmp_assort) county         on city.city_id = county.city_id     left outer join         ( select loc_id as province_id , loc_name as province_name from tmp.tmp_assort) province         on city.province_id = province.province_id where coalesce(county.county_id,'') <> '' ) q";
		String sql = "SeLECT      d.shop_id,      s.ads,      s.edm,      s.unions FROM (        select pop_id,        max(case when privilige_code='ads:login' then 'login' END) as ads,        max(case when privilige_code='edm:login' then 'login' END) as edm,        max(case when privilige_code='union:login' then 'login' END) as unions  from bdm.bdm_mmp_mk_pop_privilige_da where dt='  + ht.data_day_str +  ' group by pop_id) S join  (select id,shop_id from fdm.fdm_pop_vender_vender_chain  where  start_date <='  + ht.data_day_str +  ' and end_date > '  + ht.data_day_str +  '    group by id,shop_id) d on s.pop_id=d.id";
		// yes String sql = "select     dim_item_fin_third_cate_id as mapping_id,     map( 'dim_item_fin_first_cate_name',dim_item_fin_zero_cate_name           ) from dim.dim_item_fin_cate_getmap";
		// yes String sql = "select     item_third_cate_cd as mapping_id,     map( 'item_first_cate_cd', item_first_cate_cd          ,'item_first_cate_name',item_first_cate_name          ,'item_second_cate_cd' ,item_second_cate_cd          ,'item_second_cate_name',item_second_cate_name          ,'item_third_cate_cd' ,item_third_cate_cd          ,'item_third_cate_name',item_third_cate_name          ) from dim.dim_sku_category";
		// String sql = "select           cast(a.dim_store_num as string)       as        dim_store_num,           concat(regexp_replace(b.delv_center_name,'配送中心',''),a.store_name)          as        dim_store_name,           cast(a.int_org_num as string)         as        store_id,           cast(a.wh_cate_desc as string )        as        wh_cate_desc,           cast(b.subd_num as string)            as        dim_subd_num,           cast(c.dim_subd_name as string)       as        dim_subd_name,           cast(b.subd_num as string)            as        subd_num,           cast(a.delv_center_num as string)     as        dim_delv_center_num,           cast(b.delv_center_name as string)    as        dim_delv_center_name,           cast(a.delv_center_num as string)     as        delv_center_num,           null                  as        sort_num,           cast(c.region_name as string)         as        region_name,           cast(a.settle_org_num as string)      as settle_org_num,           cast(a.settle_org_name as string)     as settle_org_name        from           (           select                concat(rpad(delv_center_num,4,'0'),lpad(int_org_num,4,'0'))  as  dim_store_num,               store_name,               int_org_num,               wh_cate_desc,               delv_center_num,               case when int_org_num=5 and delv_center_num=6                      then '620'                    when int_org_num=5 and delv_center_num=3                      then '624'                     when int_org_num=5 and delv_center_num=10                      then '623'                    when int_org_num=5 and delv_center_num=4                      then '613'                    when int_org_num=5 and delv_center_num=5                      then '622'                    when int_org_num=5 and delv_center_num=9                      then '621'                    else  subd_num                      end as settle_org_num,                  case when int_org_num=5 and delv_center_num=6                      then '图书运营北京分公司'                    when int_org_num=5 and delv_center_num=3                      then '图书运营上海分公司'                     when int_org_num=5 and delv_center_num=10                      then '图书运营广州分公司'                    when int_org_num=5 and delv_center_num=4                      then '图书运营成都分公司'                    when int_org_num=5 and delv_center_num=5                      then '图书运营武汉分公司'                    when int_org_num=5 and delv_center_num=9                      then '图书运营沈阳分公司'                    else  subd_name                      end as settle_org_name              from (                         select                                 stores.int_org_num,                                 stores.store_name,                                 stores.delv_center_num,                                 stores.src_sys_cd,                                 stores.wh_cate_desc,                                 stores.wms_wh_cd,                                 stores.wms_wh_name,                                 stores.wms_wh_type,                                 stores.store_addr,                                 stores.store_contact,                                 stores.store_tel,                                 stores.store_mobile_no,                                 stores.wms_ver,                                 stores.subd_num,                                 stores.subd_name,                                 stores.region_name,                                 store_maint.wms2_store_id,                                 delv_center.delv_center_brevity_cd                         from                         ( select                                 storeid           as  int_org_num,                                 storename         as  store_name,                                 delivercentercode as  delv_center_num,                                 null                as  src_sys_cd,                                 storetype         as  wh_cate_desc,                                 wareno            as  wms_wh_cd,                                 warename          as  wms_wh_name,                                 locno             as  wms_wh_type,                                 addr              as  store_addr,                                 contact           as  store_contact,                                 tel               as  store_tel,                                 mobile            as  store_mobile_no,                                 wmsversion        as  wms_ver,                                 cast(mcustno as string) as subd_num,                                 mcustname          as   subd_name,                                 area               as   region_name,                                 mcustno                         from fdm.fdm_erp_wareinfo_chain                             where start_date <= '    + ht.date_today +    '                                 and end_date > '    + ht.date_tomorrow +    '                         ) stores                         left outer join                             ( select int_org_num,                                     wms2_store_id,                                     delv_center_num                             from tmp.tmp_d02_store_maint                             ) store_maint on stores.int_org_num= store_maint.int_org_num and stores.delv_center_num= store_maint.delv_center_num                         left outer join                         ( select delv_center_brevity_cd                                 ,delv_center_num                             from dim.dim_d99_delv_center_brevity_cd                         ) delv_center on delv_center.delv_center_num= stores.delv_center_num                     ) tmp_d02_store             )a         join         (             select  subd_num,                     dim_delv_center_name as delv_center_name,                     delv_center_num             from dim.dim_delv_center         )  b on a.delv_center_num=b.delv_center_num         join         ( select dim_subd_name,                 subd_num,                 Region_Name             from dim.dim_subd         ) c on b.subd_num=c.subd_num";
		//String sql = "select         '   + ht.data_day_str +    '                                                  as stat_date         ,sku.item_sku_id                                                            as item_sku_id         ,sku.item_name                                                              as item_name          ,sku.item_id                                                                as item_id          ,sku.brandname                                                              as product_name                ,sku.item_first_cate_cd                                                     as item_first_cate_cd         ,sku.item_first_cate_name                                                   as item_first_cate_name         ,sku.item_second_cate_cd                                                    as item_second_cate_cd         ,sku.item_second_cate_name                                                  as item_second_cate_name         ,sku.item_third_cate_cd                                                     as item_third_cate_cd         ,sku.item_third_cate_name                                                   as item_third_cate_name         ,sku.item_status_cd                                                         as item_status_cd         ,sku.pur_dist_flag                                                          as pur_dist_flag         ,sku.pop_coop_mode_cd                                                       as pop_coop_mode_cd         ,sku.pop_vender_id                                                          as pop_vender_id         ,sku.pop_vender_name                                                        as pop_vender_name         ,sku.pop_vender_status_cd                                                   as pop_vender_status_cd         ,sku.pop_vender_status_name                                                 as pop_vender_status_name         ,sku.shop_id                                                                as shop_id         ,sku.shop_name                                                              as shop_name         ,sku.major_supp_brevity_code                                                as major_supp_brevity_code         ,sku.purchaser_num                                                          as purchaser_num         ,sku.purchaser_name                                                         as purchaser_name         ,sku.len                                                                    as len                                                     ,sku.width                                                                  as width         ,sku.height                                                                 as height         ,sku.calc_volume                                                            as calc_volume         ,sku.size                                                                   as size         ,sku.wt                                                                     as wt         ,sku.pop_flag                                                               as pop_flag         ,sku.valid_flag                                                             as valid_flag         ,sku.support_cash_on_deliver_flag                                           as support_cash_on_deliver_flag         ,orders.new_order_quantity                                                  as new_order_quantity         ,orders.new_order_amount                                                    as new_order_amount                                 ,orders1.cancel_order_quantity                                              as cancel_order_quantity         ,orders1.cancel_order_amount                                                as cancel_order_amount         ,orders1.cancel_order_user_quantity                                         as cancel_order_user_quantity         ,orders2.finished_order_quantity                                            as finished_order_quantity         ,orders2.finished_order_amount                                              as finished_order_amount         ,repair_new.quantity                                                        as repair_new_quantity         ,repair_new.amount                                                          as repair_new_amount         ,repair_received.repair_received_quantity                                   as repair_received_quantity         ,repair_received.repair_received_amount                                     as repair_received_amount            ,repair_paid.repair_paid_quantity                                           as repair_paid_quantity         ,repair_paid.repair_paid_amount                                             as repair_paid_amount                 ,0                                                                          as page_view_quantity         ,0                                                                          as view_user_quantity           ,score.score_1_quantity                                                     as score_1_quantity         ,score.score_2_quantity                                                     as score_2_quantity           ,score.score_3_quantity                                                     as score_3_quantity         ,score.score_4_quantity                                                     as score_4_quantity           ,score.score_5_quantity                                                     as score_5_quantity          ,core_stock.numk                                                            as numk         ,sw1.zixun                                                                  as zixun         ,sw1.huizixun                                                               as huizixun                from     (         select                item_sku_id                                                                          ,item_name                                                                            ,item_id                                                                              ,brandname                                                                                ,item_first_cate_cd                                                                    ,item_first_cate_name                                                                 ,item_second_cate_cd                                                                   ,item_second_cate_name                                                                ,item_third_cate_cd                                                                  ,item_third_cate_name                                                                 ,item_status_cd                                                                      ,pur_dist_flag                ,pop_coop_mode_cd                                                                                                                                            ,pop_vender_id                                                                          ,pop_vender_name                                                                         ,pop_vender_status_cd                                                                     ,pop_vender_status_name                                                                  ,shop_id                                                                                 ,shop_name                                                                               ,major_supp_brevity_code                                                                  ,purchaser_num                                                                             ,purchaser_name                                                                            ,len                                                                                                                             ,width                                                                                ,height                                                                                ,calc_volume                                                                           ,size                                                                                 ,wt                  ,pop_flag                                                                                ,valid_flag                                                                           ,support_cash_on_deliver_flag                              from gdm.gdm_sku_basic_attrib_da         where dt='   +ht.data_day_str+   '     )sku      left outer join     (           select item_sku_id             ,count(distinct sale_ord_id) as new_order_quantity             ,sum(after_prefr_amount) as new_order_amount           from gdm_m04_ord_det_sum  where to_date(sale_ord_tm)='   + ht.data_day_str +    '              group by              item_sku_id           )orders on  sku.item_sku_id=orders.item_sku_id               left outer join     (           select item_sku_id             ,count(distinct sale_ord_id) as cancel_order_quantity             ,sum(after_prefr_amount) as cancel_order_amount                             ,count(distinct user_log_acct) as cancel_order_user_quantity           from gdm_m04_ord_det_sum  where     to_date(ord_cancel_tm)='   + ht.data_day_str +    '              group by              item_sku_id           )orders1 on  sku.item_sku_id=orders1.item_sku_id               left outer join     (           select item_sku_id              ,count(distinct  sale_ord_id) as finished_order_quantity             ,sum(after_prefr_amount) as finished_order_amount          from gdm_m04_ord_det_sum   where  to_date(Ord_Complete_Tm)='   + ht.data_day_str +    '   and sale_ord_valid_flag=1         group by              item_sku_id           )orders2 on  sku.item_sku_id=orders.item_sku_id         left outer join          (      select             ware_id  as sku_id,                count(afs_service_detail_id) as quantity,               sum(pay_price)               as amount            from fdm.fdm_afs_afs_service_detail_chain      where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    ' and ware_type=10      group by ware_id                ) repair_new  on sku.item_sku_id=repair_new.sku_id                 left outer join     (                   select              T1.ware_id  as sku_id,                 count(distinct T1.afs_service_id)      as repair_received_quantity,                                 sum(T2.cost_price)                     as repair_received_amount                           from (select * from fdm.fdm_afs_afs_service_detail_chain                              where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    ' and ware_type=10) T1        join (select * from fdm.fdm_afs_part_receive_chain                where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    'and  to_date(afs_apply_time)='   + ht.data_day_str +    ') T2         on T1.afs_service_id=T2.afs_service_id        group by T1.ware_id     )repair_received on sku.item_sku_id = repair_received.sku_id               left outer join     (                   select                 T1.ware_id  as sku_id,                    count(distinct T1.afs_service_id)   as repair_paid_quantity,                         sum(T2.suggest_amount)                  as repair_paid_amount                       from (select * from fdm.fdm_afs_afs_service_chain                 where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    '  and approve_result=21 ) T1          join (select * from fdm.fdm_afs_afs_refund_chain               where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    '  and  to_date(create_date)='   + ht.data_day_str +    ' ) T2           on T1.afs_service_id=T2.afs_service_id          group by T1.ware_id     )repair_paid on sku.item_sku_id = repair_received.sku_id                            left outer join     (       select referenceid as sku_id,              sum( case when score = 1 then 1 else 0 end ) as score_1_quantity,            sum( case when score = 2 then 1 else 0 end )   as score_2_quantity,              sum( case when score = 3 then 1 else 0 end ) as score_3_quantity,              sum( case when score = 4 then 1 else 0 end ) as score_4_quantity,              sum( case when score = 5 then 1 else 0 end ) as score_5_quantity         from fdm.fdm_club_comment_chain where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    '  and to_date(creationtime) = '   + ht.data_day_str +    '          group by referenceid     ) score on sku.item_sku_id=score.sku_id                    left outer join     (         select              wid            ,sum( numrk - numck) as numk        from fdm.fdm_pek_core_stockstatday        where dt = '   + ht.data_day_str +    '        group by wid     )core_stock on  sku.item_sku_id=core_stock.wid       left outer join     (         SELECT               sw.wid               ,sum(case when sw.sfid=0 then 1 else 0 end ) AS zixun                ,sum(case when sw.sfid !=1 then 1 else 0 end ) AS huizixun          FROM fdm.fdm_club_sayword_chain sw          where start_date <='   + ht.data_day_str +    ' and end_date >'    + ht.data_day_str +    '  and  sw.syn=1  and to_date(sw.ordertime) = '   + ht.data_day_str +    '         group by sw.wid     )sw1 on   sku.item_sku_id=sw1.wid ";
		//String sql = "SELECT    CASE WHEN m.cata_id IS NULL THEN n.cata_id ELSE m.cata_id END AS cata_id,   n.att_id,   n.att_name,   b.value_id,   b.value_name,   n.type FROM  (SELECT att_id,att_name,cata_id,group_id,type FROM fdm.fdm_forest_attribute_chain WHERE dp='ACTIVE') n    LEFT OUTER JOIN (SELECT cata_id,group_id ,type FROM fdm.fdm_forest_attribute_group_chain WHERE dp='ACTIVE') m    ON n.group_id=m.group_id AND n.type=m.type  LEFT OUTER JOIN (SELECT att_id,value_id, value_name,type FROM fdm.fdm_forest_attribute_value_chain WHERE dp='ACTIVE') b   ON n.att_id=b.att_id AND n.type=b.type; ";
		//String sql = "SELECT    n.cata_id,   n.att_id,   n.att_name,   n.value_id,   n.value_name,   SUM(d.pay_amount) as pays,     COUNT(DISTINCT d.user_id) as uids,     COUNT(DISTINCT e.shop_id) as shops,     COUNT(DISTINCT s.product_sku_id)  as sks   FROM  (SELECT * FROM dim.dim_odp_ind_prd_attr) n   LEFT OUTER JOIN (SELECT product_sku_id,prop_type,    CASE WHEN prop_type=3 THEN SPLIT(atts,':')  ELSE SPLIT(atts,':') END AS value_id FROM fdm.fdm_product_pop_property_chain   WHERE dp='ACTIVE'  ) s   ON n.att_id=s.att_id AND n.value_id=s.value_id AND s.prop_type=n.type LEFT OUTER JOIN (SELECT DISTINCT item_id,shop_id FROM gdm.gdm_m03_item_sku_da WHERE dt='2014-08-10'   ) e   ON s.product_sku_id=e.item_id JOIN (SELECT       item_id,         sale_qtty,        after_prefr_amount + sku_freight_amount AS pay_amount,        user_log_acct AS user_id    FROM adm.adm_s14_ol_shop_orders_det  WHERE dt = '2014-08-10' AND is_deal_ord = 1 AND item_id IS NOT NULL ) d     ON s.product_sku_id = d.item_id WHERE n.value_name!='品牌' AND n.att_name!='品牌' GROUP BY   n.cata_id,   n.att_id,   n.att_name,   n.value_id,   n.value_name; ";
//		String sql = "create table xxx  select * from tmp.xxxsdfs abc";
//		sql = "insert overwrite table dim.dim_odp_ind_prd_attr partition (dt='2014-07-01') select * from tmp.xxxsdfs abc";
  		sql = "USE tmp;CREATE TABLE tmp_odp_ind_prd_attr_abc  AS select * from tmp.xxxsdfs abc";
		if(sql.toLowerCase().indexOf("create")>-1 || sql.toLowerCase().indexOf("insert") > -1) {
			sql = sql.substring(sql.toLowerCase().indexOf("select"));
			sqlDecomposition(sql, true);
		} else {
			sqlDecomposition(sql);
		}
	}
	/**
	 * 
	 * @param query
	 * @param isPrefix
	 * @throws InterruptedException
	 */
	public static void sqlDecomposition(String query, boolean isPrefix) throws InterruptedException {

		Map<String, AnalysisPool> apMap = new HashMap<String, AnalysisPool>();
		StringBuffer from = new StringBuffer();
		// parser得到AST
		SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(query, JdbcUtils.HIVE);
		List<SQLStatement> stmtList = parser.parseStatementList(); 
		// 将AST通过visitor输出
		SQLASTOutputVisitor visitor = SQLUtils.createFormatOutputVisitor(from, stmtList, JdbcUtils.HIVE);
		List<SQLSelectItem> sL = new ArrayList<SQLSelectItem>();
		SQLStatement stmt = stmtList.iterator().next();
		// if(stmt instanceof SQLSelectStatement){
		SQLSelectStatement sstmt = (SQLSelectStatement) stmt;
		SQLSelect sqlselect = sstmt.getSelect();
		SQLSelectQueryBlock sqb = (SQLSelectQueryBlock) sqlselect.getQuery();
		SQLTableSource fromx = sqb.getFrom();
		fromx.accept(visitor);
		//query.getWhere().accept(whereVisitor);
		sL = sqb.getSelectList();
		// }
		StringBuffer sb = new StringBuffer();
		for (SQLSelectItem sqi : sL) {
			if (sqi.getExpr() instanceof SQLCaseExpr) {
				sb.append(sqi.getAlias()).append(",");
			} else {
				sb.append(sqi.toString()).append(",");
			}

		}
		Thread.sleep(500);
		System.out.println("*****************分析结果*********************************");
		Thread.sleep(500);
		System.out.println("* 结果列表 : "
				+ sb.toString().substring(0, sb.toString().length() - 1));
		Thread.sleep(500);
		System.out.println("*\n*****************来源列表*********************************");
		sourceAnalysis(fromx, apMap);
		Thread.sleep(500);
		System.out.println("*****************分析结束*********************************");
	
	}
	
	/**
	 * SQL分解动作
	 * @param query
	 * @throws InterruptedException 
	 */
	public static void sqlDecomposition(String query) throws InterruptedException {
		sqlDecomposition(query, false);
	}

	public static void sourceAnalysis(SQLTableSource from,
			Map<String, AnalysisPool> apMap) {
		if (from instanceof SQLExprTableSource) {
			System.out.println("真实表名: "+from.toString());
		} else if (from instanceof SQLJoinTableSource) {
			SQLTableSource left = ((SQLJoinTableSource) from).getLeft();
			SQLTableSource right = ((SQLJoinTableSource) from).getRight();
			if (left.getAlias() != null)
				print(left);
			print(right);
			sourceAnalysis(left, apMap);
			sourceAnalysis(right, apMap);
		} else if (from instanceof SQLSubqueryTableSource) {

		}
	}

	public static void JudgeExpc() {
		
	}
	
	public static void print(SQLTableSource sql) {
		String tableName = ((SQLSelectQueryBlock) ((SQLSubqueryTableSource) sql)
				.getSelect().getQuery()).getFrom().toString();
		List<SQLSelectItem> itemList = ((SQLSelectQueryBlock) ((SQLSubqueryTableSource) sql)
				.getSelect().getQuery())
				.getSelectList();
		StringBuffer sb = new StringBuffer();
		SQLExpr sqlExpr = null;
		for (SQLSelectItem item : itemList) {
//			SQLAggregateExpr.class
//			SQLAllColumnExpr.class
//			SQLAllExpr.class
//			SQLAnyExpr.class
//			SQLBetweenExpr.class
//			SQLBinaryOperator.class
//			SQLBinaryOpExpr.class
//			SQLBooleanExpr.class
//			SQLCaseExpr.class
//			SQLCastExpr.class
//			SQLCharExpr.class
//			SQLCurrentOfCursorExpr.class
//			SQLDefaultExpr.class
//			SQLExistsExpr.class
//			SQLHexExpr.class
//			SQLIdentifierExpr.class
//			SQLInListExpr.class
//			SQLInSubQueryExpr.class
//			SQLIntegerExpr.class
//			SQLListExpr.class
//			SQLLiteralExpr.class
//			SQLMethodInvokeExpr.class
//			SQLNCharExpr.class
//			SQLNotExpr.class
//			SQLNullExpr.class
//			SQLNumberExpr.class
//			SQLNumericLiteralExpr.class
//			SQLPropertyExpr.class
//			SQLQueryExpr.class
//			SQLSomeExpr.class
//			SQLTextLiteralExpr.class
//			SQLUnaryExpr.class
//			SQLUnaryOperator.class
//			SQLVariantRefExpr.class ...................
			sqlExpr = item.getExpr();
			if(sqlExpr instanceof SQLAggregateExpr) {
			} else if (sqlExpr instanceof SQLIdentifierExpr) {
				sb.append(((SQLIdentifierExpr)sqlExpr).getName()).append(",");
			}
			sb.append(item.getAlias()).append(",");
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out
		.println("* 真实表名: " + tableName + "\t別名:"
				+ sql.getAlias()
				+  "\n* 获取列:\t"
				+ itemList.toString());
	}
	public static String getpPrefix() {
		return pPrefix;
	}
	public static void setpPrefix(String pPrefix) {
		SqlExplain.pPrefix = pPrefix;
	}
}


class AnalysisPool {
	private String select;
	private int sort; // 排序
	private String parent;
	public String getSelect() {
		return select;
	}
	public void setSelect(String select) {
		this.select = select;
	}
	public int getSort() {
		return sort;
	}
	public void setSort(int sort) {
		this.sort = sort;
	}
	public String getParent() {
		return parent;
	}
	public void setParent(String parent) {
		this.parent = parent;
	}

}
