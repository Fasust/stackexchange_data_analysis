words = LOAD '/user/root/pig_data/popularwords.txt' as (word:chararray);
groupedByWord = group words by word;
counted = FOREACH groupedByWord GENERATE group as word,COUNT(words) as cnt;
result = FOREACH counted GENERATE word, cnt;
store result into '/user/root/outputpig/popularwords.txt';