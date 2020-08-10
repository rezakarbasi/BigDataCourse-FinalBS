SELECT CHANNEL_ID, 
       arrayJoin(hashtags AS src) AS Hashtag, 
       COUNT(arrayJoin(hashtags AS src) AS Hashtag) AS hashtag_cnt
FROM Channel.data
WHERE empty(Hashtag) == 0
GROUP BY Hashtag, CHANNEL_ID
ORDER By CHANNEL_ID ,hashtag_cnt DESC 
LIMIT 1 BY CHANNEL_ID