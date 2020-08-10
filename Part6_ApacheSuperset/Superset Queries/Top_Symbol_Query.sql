SELECT CHANNEL_ID, 
       arrayJoin(symbols AS src) AS Symbol, 
       COUNT(arrayJoin(symbols AS src) AS Symbol) AS Symbol_cnt
FROM Channel.data
WHERE empty(Symbol) == 0
GROUP BY Symbol, CHANNEL_ID
ORDER By CHANNEL_ID ,Symbol_cnt DESC 
LIMIT 1 BY CHANNEL_ID