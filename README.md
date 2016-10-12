# spark-samples
Repository for spark samples

Run the word count program as (Make sure '/user/user01/output' doesn't exist already)
 /opt/mapr/spark/spark-1.6.1/bin/spark-submit --class example.wordcount.JavaWordCount --master yarn sparkwordcount-1.0.jar /user/user01/input/11.txt /user/user01/output