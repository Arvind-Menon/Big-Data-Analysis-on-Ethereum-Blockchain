import pyspark
import time
sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        int(fields[6])
        
        return True
    except:
        return False

def get_date(line):
    fields = line.split(',')
    time_epoch = int(fields[6])
    monthYear = time.strftime("%m-%Y",time.gmtime(time_epoch))
    
    return(monthYear, 1)

#get_date(1541290680)
lines = sc.textFile("/data/ethereum/transactions")

clean_lines = lines.filter(is_good_line)
key = clean_lines.map(get_date).persist()
result = key.reduceByKey(lambda a,b: a+b).sortByKey()

result.saveAsTextFile("/user/arm34/PartA_final")


