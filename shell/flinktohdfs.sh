命令提交
./bin/flink run-application \
-d \
-t yarn-application \
-ynm DbusCdc2DimHbase \
-yjm 900 \
-ytm 900 \
-yqu root.default \
-c com.lzy.stream.realtime.v2.app.dim.BaseApp.java /root/stream_realtime-1.0-SNAPSHOT.jar