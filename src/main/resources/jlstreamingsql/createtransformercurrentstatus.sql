drop table if exists transformer_current_status;
create table if not exists transformer_current_status(
mp_id bigint comment '测量点标识',
time timestamp not null default CURRENT_TIMESTAMP comment '记录时刻',
ths timestamp not null default CURRENT_TIMESTAMP comment '重载开始时刻',
tos timestamp not null default CURRENT_TIMESTAMP comment '过载开始时刻',
tos1 timestamp not null default CURRENT_TIMESTAMP comment '长时过载标志1',
tos2 timestamp not null default CURRENT_TIMESTAMP comment '长时过载标志2',
tos3 timestamp not null default CURRENT_TIMESTAMP comment '长时过载标志3',
tos4 timestamp not null default CURRENT_TIMESTAMP comment '长时过载标志4',
tos5 timestamp not null default CURRENT_TIMESTAMP comment '长时过载标志5',
thd bigint comment '重载持续时间',
tod bigint comment '过载持续时间',
tod1 bigint comment '长时过载持续时间1',
tod2 bigint comment '长时过载持续时间2',
tod3 bigint comment '长时过载持续时间3',
tod4 bigint comment '长时过载持续时间4',
tod5 bigint comment '长时过载持续时间5',
status int(1) comment '当前状态标,   0表示正常，1表示重载，2表示过载，3表示负载率计算异常，容量为空或最大负荷不能计算，4表示迟到数据',
overLoadstatus int(1) comment '过载状态标志, 0表示无过载，1表示短时过载，2表示长时过载,3表示短时过载但本次过载曾经出现长时过载情况',
loadrate Decimal(8,4) comment '负载率'
)ENGINE=InnoDB DEFAULT CHARSET=utf8;