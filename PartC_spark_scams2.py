import pyspark
import time
sc=pyspark.SparkContext()
#clean_transactions function to filter out bad lines from transactions dataset
def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
        int(fields[6])
        float(int(fields[3]))
        return True

    except:
        return False
#read transactions dataset
transactions = sc.textFile('/data/ethereum/transactions')
#remove bad lines from transactions dataset using clean_transactions function
transactions_filter = transactions.filter(clean_transactions)
#map the to_address and value field from transactions dataset
transactions_join = transactions_filter.map(lambda l: (l.split(',')[2] , (int(l.split(',')[6]), float(int(l.split(',')[3]))))).persist()
#read scams dataset
scams = sc.textFile('/user/arm34/scams.csv')
#
scams_join = scams.map(lambda f: (f.split(',')[0],f.split(',')[5], f.split(',')[6]))

joined_data = transactions_join.join(scams_join)
#joined_data.saveAsTextFile('scamsjoinnew')

category = joined_data.map(lambda a: (a[1][1], a[1][0][1]))
category_sum = category.reduceByKey(lambda a,b: (a+b)).sortByKey()
category_sum.saveAsTextFile('most_lucrative_scam3')

time_series = joined_data.map(lambda b: ((b[1][1], time.strftime("%m-%Y",time.gmtime(b[1][0][0]))), b[1][0][1],b[1][0][2]))
time_series_sum = time_series.reduceByKey(lambda a,b: (a+b)).sortByKey()
time_series_sum.saveAsTextFile('timeseries_trial3')
