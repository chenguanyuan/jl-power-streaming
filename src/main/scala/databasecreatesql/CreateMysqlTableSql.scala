package databasecreatesql

object CreateMysqlTableSql {

  //创建原始数据日表transformer_sourcedata_day,用于记录从计量读取的二次值
  def createTransformerSourcedataDaySql(date:String):String={
    s"create table if not exists transformer_sourcedata_day_$date"+
      scala.io.Source
        .fromURL(this.getClass.getResource("/jlstreamingsql/transformer_sourcedata_day_tablefields")).mkString
  }

  //创建一次值记录日表transformer_primarysidedata_day
  def createTransformerPrimarysidedataDaySql(date:String):String={
    s"create table if not exists transformer_primarysidedata_day_$date"+
      scala.io.Source
        .fromURL(this.getClass.getResource("/jlstreamingsql/transformer_primarysidedata_day_tablefields")).mkString
  }

  //创建变压器时刻状态日表transformer_ins_status_day，记录所有数据对应的时刻状态
  def createTransformerInsStatusSql(date:String):String={
    s"create table if not exists transformer_ins_status_day_$date"+
      scala.io.Source
        .fromURL(this.getClass.getResource("/jlstreamingsql/transformer_ins_status_day_tablefields")).mkString
  }

  //创建变压器重过载日表transformer_exceed_status，已经确认为重过载的记录
  def createTransformerExceedStatus(date:String):String={
    s"create table if not exists transformer_exceed_status_$date"+
      scala.io.Source
        .fromURL(this.getClass.getResource("/jlstreamingsql/transformer_exceed_status_tablefields")).mkString
  }

}
