/* table to store test data */
create table md5_test (
 i int not null,
 input_data varchar(256) character set latin not null,
 output_data char(32) character set latin not null
) unique primary index (i);
