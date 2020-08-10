SELECT CHANNEL_ID, TYPE, COUNT (TYPE) AS Type_Cnt, toYYYYMMDDhhmmss(now()) AS NowTime
FROM Channel.data
WHERE (NowTime - myTime) <= 1000000 AND (NowTime - myTime) >= 0
GROUP BY TYPE, CHANNEL_ID
ORDER BY CHANNEL_ID, TYPE