hql-parser
==========

hive sql parser


分析HQL语句到字段级别。


解析抽象语法树，一条hql被第一个关键字FROM分成2部分，前部分叫做Insert，后半部分叫做From


1. 当不指定目标表是，默认插入到tmp dir


2. From 源表有几种情况


2.1 直接FROM 表


2.2 子查询


2.3 JOIN


....递归便利获得所有表、字段。


待续.

