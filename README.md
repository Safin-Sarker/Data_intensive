
# Data_intensive




All of our codes are in coding_files folder of name node. to see the list of code please follow the follwing code



```bash
  $ cd /coding_files
  $ ls -l
```
All of our raw dataset are in the datafile folder in the name node
```bash
$ cd /datafile
$ ls -l
```

To run the mappers and reducer follow the follwing command
```bash
python3  MRJOB.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///datafile/DAT500_Dataset/DAT500_Dataset.csv --output-dir hdfs:///output/output1 --no-output
```

To run the ml_plot runs in local (namenode)
```bash
$ cd coding_files
$ python3 ml_plot.py
```

To run the spark algorithm
```bash
$ cd coding_files
$ spark-submit --master yarn --executor-cores 4 --num-executors 4 --executor-memory 6g --driver-memory 1g --conf spark.kryoserializer.buffer.max=512m spark-algo.py
```