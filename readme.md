https://github.com/iizrailevsky/hadoop-hello-world

mvn clean install; hadoop fs -rm -r /output; hadoop jar target/db2gb.jar main.java.MachineSensorData /input/data /output; hadoop fs -cat /output/part-r-00000