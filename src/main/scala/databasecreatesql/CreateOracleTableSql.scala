package databasecreatesql

object CreateOracleTableSql {
  //创建原始数据日表transformer_sourcedata_day,用于记录从计量读取的二次值
  def createTransformerSourcedataDaySql(date:String):String={
    s"""
       |declare
       |tablename number;
       |begin
       |select count(1) into tablename from dba_tables where table_name = upper('transformer_sourcedata_day_$date');
       |if tablename<1 then
       | execute immediate '
       | create table transformer_sourcedata_day_$date (
       | mp_id NUMBER(32,0),
       | time timestamp,
       | ZYGGL NUMBER(8,4),
       | AZXYG NUMBER(8,4),
       | BZXYG NUMBER(8,4),
       | CZXYG NUMBER(8,4),
       | ZWGGL NUMBER(8,4),
       | AZXWG NUMBER(8,4),
       | BZXWG NUMBER(8,4),
       | CZXWG NUMBER(8,4),
       | SZGL NUMBER(8,4),
       | ASZGL NUMBER(8,4),
       | BSZGL NUMBER(8,4),
       | CSZGL NUMBER(8,4),
       | ZGLYS NUMBER(8,4),
       | AGLYS NUMBER(8,4),
       | BGLYS NUMBER(8,4),
       | CGLYS NUMBER(8,4))';
       | execute immediate 'comment on table transformer_sourcedata_day_$date is ''原始数据日表transformer_sourcedata_day,用于记录从计量读取的二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.mp_id is ''测量点标识''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.time is ''记录时刻''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.ZYGGL is ''总有功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.AZXYG is ''A相有功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.BZXYG is ''B相有功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.CZXYG is ''C相有功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.ZWGGL is ''总无功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.AZXWG is ''A相无功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.BZXWG is ''B相无功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.CZXWG is ''C相无功功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.SZGL is ''视在功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.ASZGL is ''A相视在功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.BSZGL is ''B相视在功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.CSZGL is ''C相视在功率，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.ZGLYS is ''总功率因数，二次值''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.AGLYS is ''A相功率因数''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.BGLYS is ''B相功率因数''';
       | execute immediate 'comment on column transformer_sourcedata_day_$date.CGLYS is ''C相功率因数''';
       | end if;
       | end;""".stripMargin
  }

  def createTransformerPrimarysidedataDaySql(date:String):String={
    s"""
       |declare
       |tablename number;
       |begin
       |select count(1) into tablename from dba_tables where table_name = upper('transformer_primarysidedata_day_$date');
       |if tablename<1 then
       | execute immediate '
       | create table transformer_primarysidedata_day_$date (
       |mp_id NUMBER(32,0),
       |time timestamp,
       |loadrate NUMBER(8,4),
       |maxpower NUMBER(8,4),
       |ZYGGL NUMBER(8,4),
       |AZXYG NUMBER(8,4),
       |BZXYG NUMBER(8,4),
       |CZXYG NUMBER(8,4),
       |ZWGGL NUMBER(8,4),
       |AZXWG NUMBER(8,4),
       |BZXWG NUMBER(8,4),
       |CZXWG NUMBER(8,4),
       |SZGL NUMBER(8,4),
       |ASZGL NUMBER(8,4),
       |BSZGL NUMBER(8,4),
       |CSZGL NUMBER(8,4),
       |ZGLYS NUMBER(8,4),
       |AGLYS NUMBER(8,4),
       |BGLYS NUMBER(8,4),
       |CGLYS NUMBER(8,4)
       |)';
       |execute immediate 'comment on table transformer_primarysidedata_day_$date is ''一次值记录日表transformer_primarysidedata_day，记录配变一次值''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.mp_id is ''测量点标识''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.time is ''记录时刻''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.loadrate is ''负载率''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.maxpower is ''最大视在功率,单位kW''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.ZYGGL is ''总有功功率,单位kW''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.AZXYG is ''A相有功功率,单位kW''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.BZXYG is ''B相有功功率,单位kW''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.CZXYG is ''C相有功功率,单位kW''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.ZWGGL is ''总无功功率,单位kVar''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.AZXWG is ''A相无功功率,单位kVar''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.BZXWG is ''B相无功功率,单位kVar''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.CZXWG is ''C相无功功率,单位kVar''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.SZGL is ''视在功率,单位kVA''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.ASZGL is ''A相视在功率,单位kVA''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.BSZGL is ''B相视在功率,单位kVA''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.CSZGL is ''C相视在功率,单位kVA''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.ZGLYS is ''总功率因数''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.AGLYS is ''A相功率因数''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.BGLYS is ''B相功率因数''';
       |execute immediate 'comment on column transformer_primarysidedata_day_$date.CGLYS is ''C相功率因数''';
       | end if;
       | end;
           """.stripMargin
  }

  def createTransformerInsStatusSql(date:String):String={
    s"""declare
       |tablename number;
       |begin
       |select count(1) into tablename from dba_tables where table_name = upper('transformer_ins_status_day_$date');
       |if tablename<1 then
       |execute immediate '
       |create table transformer_ins_status_day_$date (
       |mp_id NUMBER(32,0),
       |time timestamp not null,
       |ths timestamp not null,
       |tos timestamp not null,
       |tos1 timestamp not null,
       |tos2 timestamp not null,
       |tos3 timestamp not null,
       |tos4 timestamp not null,
       |tos5 timestamp not null,
       |thd NUMBER(32，0),
       |tod NUMBER(32，0),
       |tod1 NUMBER(32，0),
       |tod2 NUMBER(32，0),
       |tod3 NUMBER(32，0),
       |tod4 NUMBER(32，0),
       |tod5 NUMBER(32，0),
       |status NUMBER(1，0),
       |overLoadstatus NUMBER(1，0),
       |loadrate NUMBER(8,4)
       |)
       |';
       |execute immediate 'comment on table transformer_ins_status_day_$date is ''变压器时刻状态日表transformer_ins_status_day，记录所有数据对应的时刻状态''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.mp_id is ''测量点标识''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.time is ''记录时刻''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.ths is ''重载开始时刻''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos is ''过载开始时刻''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos1 is ''长时过载标志1;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos2 is ''长时过载标志2;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos3 is ''长时过载标志3;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos4 is ''长时过载标志4;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tos5 is ''长时过载标志5;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.thd is ''重载持续时间，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod is ''过载持续时间，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod1 is ''长时过载持续时间1，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod2 is ''长时过载持续时间2，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod3 is ''长时过载持续时间3，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod4 is ''长时过载持续时间4，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.tod5 is ''长时过载持续时间5，单位为min''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.status is ''当前状态标,   0表示正常，1表示重载，2表示过载，3表示负载率计算异常，容量为空或最大负荷不能计算，4表示迟到数据''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.overLoadstatus is ''过载状态标志, 0表示无过载，1表示短时过载，2表示长时过载,3表示短时过载但本次过载曾经出现长时过载情况''';
       |execute immediate 'comment on column transformer_ins_status_day_$date.loadrate is ''负载率''';
       |end if;
       |end;
           """.stripMargin
  }

  def createTransformerExceedStatus(date:String):String={
    s"""declare
       |tablename number;
       |begin
       |select count(1) into tablename from dba_tables where table_name = upper('transformer_exceed_status_$date');
       |if tablename<1 then
       |execute immediate '
       |create table transformer_exceed_status_$date (
       |mp_id NUMBER(32,0),
       |status NUMBER(1，0),
       |overLoadstatus NUMBER(1，0),
       |ts timestamp not null,
       |te timestamp not null,
       |td NUMBER(32,0)
       |)
       |';
       |execute immediate 'comment on table transformer_exceed_status_$date is ''变压器重过载日表transformer_exceed_status，已经确认为重过载的记录''';
       |execute immediate 'comment on column transformer_exceed_status_$date.mp_id is ''测量点标识''';
       |execute immediate 'comment on column transformer_exceed_status_$date.status is ''当前状态标, 0表示正常，1表示重载，2表示过载，3表示负载率计算异常，容量为空或最大负荷不能计算，4表示迟到数据''';
       |execute immediate 'comment on column transformer_exceed_status_$date.overLoadstatus is ''过载状态标志, 0表示无过载，1表示短时过载，2表示长时过载,3表示短时过载但本次过载曾经出现长时过载情况''';
       |execute immediate 'comment on column transformer_exceed_status_$date.ts is ''开始时刻''';
       |execute immediate 'comment on column transformer_exceed_status_$date.te is ''结束时刻''';
       |execute immediate 'comment on column transformer_exceed_status_$date.td is ''持续时间，单位为Min''';
       | end if;
       | end;
         """.stripMargin
  }

  def createTransformerCurrentStatus():String={
    s"""declare
       |tablename number;
       |begin
       |select count(1) into tablename from dba_tables where table_name = upper('transformer_current_status');
       |if tablename<1 then
       |execute immediate '
       |create table transformer_current_status (
       |mp_id NUMBER(32,0) PRIMARY KEY,
       |time timestamp not null,
       |ths timestamp not null,
       |tos timestamp not null,
       |tos1 timestamp not null,
       |tos2 timestamp not null,
       |tos3 timestamp not null,
       |tos4 timestamp not null,
       |tos5 timestamp not null,
       |thd NUMBER(32，0),
       |tod NUMBER(32，0),
       |tod1 NUMBER(32，0),
       |tod2 NUMBER(32，0),
       |tod3 NUMBER(32，0),
       |tod4 NUMBER(32，0),
       |tod5 NUMBER(32，0),
       |status NUMBER(1，0),
       |overLoadstatus NUMBER(1，0),
       |loadrate NUMBER(8,4)
       |)
       |';
       |execute immediate 'comment on table transformer_current_status is ''配变最新时刻状态表transformer_current_status，记录配变当前最新时刻的状态''';
       |execute immediate 'comment on column transformer_current_status.mp_id is ''测量点标识''';
       |execute immediate 'comment on column transformer_current_status.time is ''记录时刻''';
       |execute immediate 'comment on column transformer_current_status.ths is ''重载开始时刻''';
       |execute immediate 'comment on column transformer_current_status.tos is ''过载开始时刻''';
       |execute immediate 'comment on column transformer_current_status.tos1 is ''长时过载标志1;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_current_status.tos2 is ''长时过载标志2;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_current_status.tos3 is ''长时过载标志3;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_current_status.tos4 is ''长时过载标志4;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_current_status.tos5 is ''长时过载标志5;1:长时过载，0：无长时过载''';
       |execute immediate 'comment on column transformer_current_status.thd is ''重载持续时间，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod is ''过载持续时间，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod1 is ''长时过载持续时间1，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod2 is ''长时过载持续时间2，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod3 is ''长时过载持续时间3，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod4 is ''长时过载持续时间4，单位为min''';
       |execute immediate 'comment on column transformer_current_status.tod5 is ''长时过载持续时间5，单位为min''';
       |execute immediate 'comment on column transformer_current_status.status is ''当前状态标,   0表示正常，1表示重载，2表示过载，3表示负载率计算异常，容量为空或最大负荷不能计算，4表示迟到数据''';
       |execute immediate 'comment on column transformer_current_status.overLoadstatus is ''过载状态标志, 0表示无过载，1表示短时过载，2表示长时过载,3表示短时过载但本次过载曾经出现长时过载情况''';
       |execute immediate 'comment on column transformer_current_status.loadrate is ''负载率''';
       |end if;
       |end;
           """.stripMargin
  }

}
