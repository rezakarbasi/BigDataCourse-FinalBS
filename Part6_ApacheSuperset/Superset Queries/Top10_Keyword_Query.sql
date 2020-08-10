SELECT CHANNEL_ID, 
       arrayJoin(keywords  AS src) AS Keyword, 
       COUNT(arrayJoin(keywords AS src) AS Keyword) AS Keyword_cnt
FROM Channel.data
WHERE empty(Keyword) == 0
GROUP BY Keyword, CHANNEL_ID
ORDER By CHANNEL_ID ,Keyword_cnt DESC 
LIMIT 10 BY CHANNEL_ID