/*
Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
Description: error navigating and troubleshooting rules
Author: DingQiang Liu
*/

drop table if exists log_message_level;
create table log_message_level(
    name varchar(30),
    expression varchar(200)
)
;

drop table if exists issue_category;
create table issue_category(
    cat_id varchar(30) primary key,
    name varchar(30),
    privilege integer,
    pattern varchar(200)
)
;

drop table if exists issue_reason;
create table issue_reason(
    reason_id varchar(30) primary key,
    reason_name varchar(30),
    issue_cat_name varchar(30),
    privilege integer,
    table_name varchar(30),
    filter_columns varchar(30),
    reason_pattern varchar(200),
    action varchar(200)
)
;

insert into log_message_level values('FATAL', ' thread_name=''SafetyShutdown'' and message=''Shutting down this node'' ');

insert into issue_category values('node_down', 'node down', 0, 'Shutting down this node');

insert into issue_reason values('node_down_network_failed', 'network failed', 'node down', 0, 'vertica_log', 'time', 'message:saw membership message 8192', 'check netowrk interfaces/switch status, and errors/dropped/overrun indictors in output of ifconfig or "ip -s link" commands.');
insert into issue_reason values('node_down_node_left_cluster', 'node left cluster', 'node down', 1, 'vertica_log', 'time', 'message:nodeSetNotifier left the cluster', Null);


insert into issue_category values('sql_syntax_error', 'SQL syntax error', 0, 'syntax error at or near');

-- commit;
