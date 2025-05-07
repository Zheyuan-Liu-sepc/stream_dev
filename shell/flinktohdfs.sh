命令提交
./bin/flink run-application \
-d \
-t yarn-application \
-ynm DbusCdc2DimHbase \
-yjm 900 \
-ytm 900 \
-yqu root.default \
-c com.lzy.stream.realtime.v2.app.dim.BaseApp.java /root/stream_realtime-1.0-SNAPSHOT.jar



./bin/flink run-application \
-t yarn-application \
-d \
-Djobmanager.memory.process.size=1024mb \
-Dtaskmanager.memory.process.size=1024mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-c com.lzy.stream.realtime.v2.app.dim.BaseApp \
/root/stream_realtime-1.0-SNAPSHOT.jar