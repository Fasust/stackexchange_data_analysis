words = LOAD '/user/root/pig_data/popularwords.txt' as (word:chararray);
groupedByWord = group words by word;
counted = FOREACH groupedByWord GENERATE group as word,COUNT(words) as cnt;
result = FOREACH counted GENERATE word, cnt;
ordered = ORDER result BY cnt DESC;
outoput = LIMIT ordered 10;
store output into '/user/root/outputpig/popularwords.txt';