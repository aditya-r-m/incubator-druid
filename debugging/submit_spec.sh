curl 'http://localhost:8888/druid/indexer/v1/supervisor' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:72.0) Gecko/20100101 Firefox/72.0' -H 'Accept: application/json, text/plain, */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Content-Type: application/json;charset=utf-8' -H 'Origin: http://localhost:8888' -H 'DNT: 1' -H 'Connection: keep-alive' -H 'Referer: http://localhost:8888/unified-console.html' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' --data '{"type":"pubsub","ioConfig":{"type":"pubsub","projectId":"druid-265211","subscription":"druid","minimumMessageTime":0,"maximumMessageTime":1577623444579,"pollTimeout":10000000},"tuningConfig":{"type":"pubsub","maxPendingPersists":10},"dataSchema":{"dataSource":"pubsub","parser":{"type":"string","parseSpec":{"format":"json","timestampSpec":{"column":"ts","format":"auto"},"dimensionsSpec":{"dimensions":[{"type":"string","name":"d"}]}}},"metricsSpec":[{"type":"count","name":"count"},{"type":"longSum","name":"total","fieldName":"v"},{"type":"HLLSketchBuild","name":"uniques","fieldName":"u"}],"granularitySpec":{"segmentGranularity":"day","queryGranularity":"none","intervals":["2013-08-31/2013-09-01"]}}}'
