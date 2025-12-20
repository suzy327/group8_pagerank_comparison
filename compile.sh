#!/bin/bash
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
export GIRAPH_JAR=/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar

rm -rf classes
mkdir classes

echo "Compiling Java sources..."
javac -cp "$HADOOP_CLASSPATH:$GIRAPH_JAR" -d classes src/*.java

echo "Building Jar..."
jar -cvf pagerank-compare.jar -C classes .

echo "✅ Compilation Complete."