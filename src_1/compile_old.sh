#!/bin/bash
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
export GIRAPH_JAR=/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar
rm -rf classes
mkdir classes
echo "Compiling..."
javac -cp "$HADOOP_CLASSPATH:$GIRAPH_JAR" -d classes src/*.java
if [ $? -eq 0 ]; then
    jar -cvf pagerank-compare.jar -C classes .
    echo "✅ Success! File: pagerank-compare.jar"
else
    echo "❌ Compilation Failed."
fi
