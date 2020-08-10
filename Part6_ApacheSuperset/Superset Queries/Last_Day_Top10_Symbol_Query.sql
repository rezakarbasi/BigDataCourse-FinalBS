SELECT CHANNEL_ID,
       toYYYYMMDDhhmmss(now()) AS NowTime,
       arrayJoin(symbols AS src) AS Symbol, 
       COUNT(arrayJoin(symbols AS src) AS Symbol) AS Symbol_cnt
FROM Channel.data
WHERE (NowTime - myTime) <= 1000000 AND (NowTime - myTime) >= 0 AND empty(Symbol) == 0
GROUP BY Symbol, CHANNEL_ID
ORDER By CHANNEL_ID ,Symbol_cnt DESC 
LIMIT 10 BY CHANNEL_ID