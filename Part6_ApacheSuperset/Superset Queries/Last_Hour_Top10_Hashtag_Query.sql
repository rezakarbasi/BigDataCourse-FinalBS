SELECT CHANNEL_ID ,
       toYYYYMMDDhhmmss(now()) AS NowTime,
       arrayJoin(hashtags AS src) AS Hashtag,
       COUNT(arrayJoin(hashtags AS src) AS Hashtag) AS hashtag_cnt
FROM Channel.data
WHERE (NowTime - myTime) <= 10000 AND (NowTime - myTime) >= 0 AND empty(Hashtag) == 0
GROUP BY Hashtag, CHANNEL_ID
ORDER By CHANNEL_ID ,hashtag_cnt DESC 
LIMIT 10 BY CHANNEL_ID