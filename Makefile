build:
	mvn package

run:
	(rm -r ./output || \
	spark-submit --class com.joe.scalaStuff.App --master local target/sparkSandbox-1.0-SNAPSHOT.jar $(file) ./output)

clean:
	mvn clean

