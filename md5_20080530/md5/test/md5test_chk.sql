/* check md5 result against test data */
sel
i,
hash_md5(input_data),
case
 when hash_md5(input_data) = output_data then 'OK'
 else 'NG'
end as "result"
from
md5_test
order by 1;
