SELECT CHANNEL_ID ,COUNT (MESSAGE_ID) AS Total_Messages, toYYYYMMDDhhmmss(now()) AS NowTime
FROM Channel.data
WHERE (NowTime - myTime) <= 1000000 AND (NowTime - myTime) >= 0
GROUP BY CHANNEL_ID 
ORDER BY CHANNEL_ID 