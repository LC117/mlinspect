create table data (data_f int, b int, c int, idd serial);

insert into data (data_f, b, c)
    select id.*, id.*, id.*
    from generate_series(1, 10000000) as id
    order by random();
