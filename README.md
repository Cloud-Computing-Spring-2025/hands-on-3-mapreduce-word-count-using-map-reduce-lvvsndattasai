# Word Count Using Hadoop MapReduce

## **Project Overview**
This project implements a **Word Count** program using **Hadoop MapReduce**. The goal is to process a text dataset, count the occurrences of each word, and output the results in descending order of frequency. The project is executed using a **Docker-based Hadoop cluster**.

---

## **Approach and Implementation**
### **1. Mapper Class (`WordMapper.java`)**
The **Mapper** processes each line of input text and emits `(word, 1)` key-value pairs.
- **Tokenization:** Splits text into words.
- **Normalization:** Converts words to lowercase and removes punctuation.
- **Key-Value Emission:** Outputs `<word, 1>` for each word.

```java
public class WordMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase()); // Normalize
            if (!word.toString().isEmpty()) {
                output.collect(word, one);
            }
        }
    }
}
```

---

### **2. Reducer Class (`WordReducer.java`)**
The **Reducer** aggregates word counts from the **Mapper** output.
- **Combines word occurrences** for each unique word.
- **Emits final `(word, count)` pairs**.

```java
public class WordReducer extends MapReduceBase 
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values, 
            OutputCollector<Text, IntWritable> output, Reporter reporter) 
            throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }
}
```

---

### **3. Job Driver (`Controller.java`)**
This class configures and **executes the MapReduce job**.
- **Defines input & output paths.**
- **Sets Mapper, Reducer, and Combiner classes.**
- **Executes the job on Hadoop.**

```java
public class Controller {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Controller.class);
        conf.setJobName("WordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordMapper.class);
        conf.setCombinerClass(WordReducer.class);
        conf.setReducerClass(WordReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
```

---

## **Execution Steps**
### **1️⃣ Start the Hadoop Cluster**
Run:
```bash
docker-compose up -d
```

### **2️⃣ Build the Project**
```bash
mvn clean install
```

### **3️⃣ Move the JAR File to a Shared Folder**
```bash
mv target/WordCountUsingHadoop-0.0.1-SNAPSHOT.jar shared-folder/input/code/
```

### **4️⃣ Copy JAR File to Hadoop Container**
```bash
docker cp shared-folder/input/code/WordCountUsingHadoop-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### **5️⃣ Prepare Input Dataset**
Create `input.txt` with the following content:
```
Big Data is amazing
Hadoop is a part of Big Data
Machine Learning and Big Data work together
Cloud Computing powers Big Data analytics
```
Copy it to the Hadoop container:
```bash
docker cp shared-folder/input/data/input.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### **6️⃣ Upload Input File to HDFS**
Inside the **Hadoop container**:
```bash
docker exec -it resourcemanager /bin/bash
hadoop fs -mkdir -p /input/dataset
hadoop fs -put /opt/hadoop-3.2.1/share/hadoop/mapreduce/input.txt /input/dataset
```

### **7️⃣ Run the MapReduce Job**
```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/WordCountUsingHadoop-0.0.1-SNAPSHOT.jar com.example.controller.Controller /input/dataset/input.txt /output
```

### **8️⃣ View the Output in HDFS**
```bash
hadoop fs -cat /output/*
```

### **9️⃣ Copy Output from HDFS to Local Machine**
```bash
hdfs dfs -get /output /opt/hadoop-3.2.1/share/hadoop/mapreduce/
exit
docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output/ shared-folder/output/
cat shared-folder/output/part-00000
```

---

## **Challenges Faced & Solutions**
| Challenge | Solution |
|-----------|----------|
| **Docker permission issues when copying files** | Used `sudo` while copying shared files. |
| **HDFS file overwrite error** | Removed `/output` before running MapReduce again (`hadoop fs -rm -r /output`). |
| **Missing `input.txt` in the container** | Ensured it was correctly copied to the correct Hadoop path before `hadoop fs -put`. |

---

## **Sample Input & Output**
### **📌 Sample Input (`input.txt`)**
```
Big Data is amazing
Hadoop is a part of Big Data
Machine Learning and Big Data work together
Cloud Computing powers Big Data analytics
```

### **📌 Expected Output**
```
Big 3
Data 3
is 1
amazing 1
Hadoop 1
a 1
part 1
Machine 1
Learning 1
and 1
work 1
together 1
Cloud 1
Computing 1
powers 1
analytics 1
```

