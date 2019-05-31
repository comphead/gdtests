CREATE KEYSPACE exam
    WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

create table exam.activity(type_ text, ip text, unix_time varint, category_id varint, id varint primary key);