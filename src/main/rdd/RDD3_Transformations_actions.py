from pyspark import SparkContext

sc = SparkContext("local[2]", "Transformations_Example")

data = [1,2,3,4,5]
original_rdd = sc.parallelize(data)
print(original_rdd.collect())
#[1, 2, 3, 4, 5]

addition_rdd = original_rdd.map(lambda x: x + 20)
print(addition_rdd.collect())
#[21, 22, 23, 24, 25]

filtered_rdd = original_rdd.filter(lambda x: x < 3)
print(filtered_rdd.collect())
#[1, 2]

input_data = ["HI I am Alex", "How are you"]
word_rdd = sc.parallelize(input_data)

map_rdd = word_rdd.map(lambda line: line.split(" "))
flatmap_rdd = word_rdd.flatMap(lambda line: line.split(" "))

print(map_rdd.collect())
#[['HI', 'I', 'am', 'Alex'], ['How', 'are', 'you']]

print(flatmap_rdd.collect())
#['HI', 'I', 'am', 'Alex', 'How', 'are', 'you']

print("count: ", original_rdd.count())
#count:  5

print("min: ",original_rdd.min())
#min:  1

print("max: ",original_rdd.max())
#max:  5

input_data1 = sc.parallelize([1,2,3,4])
input_data2 = sc.parallelize([3,4,5,6,7])

union_rdd = input_data1.union(input_data2)
print("union_rdd: ",union_rdd.collect())
#union_rdd:  [1, 2, 3, 4, 3, 4, 5, 6, 7]

distinct_rdd = union_rdd.distinct()
print("distinct_rdd: ",distinct_rdd.collect())
#distinct_rdd:  [4, 1, 5, 2, 6, 3, 7]

input_tuple_data = [("Cake", 1), ("Fruits", 2), ("Groceries", 3), ("Fruits", 4), ("Cake", 6)]
new_rdd = sc.parallelize(input_tuple_data)
grouped_rdd = new_rdd.groupByKey()

collected_data = grouped_rdd.collect()
for key, values in collected_data:
    print("groupByKey key: ", key)
    print("groupByKey value: ", list(values))
'''
groupByKey key:  Fruits
groupByKey value:  [2, 4]

groupByKey key:  Groceries
groupByKey value:  [3]

groupByKey key:  Cake
groupByKey value:  [1, 6]
'''

group_by_rdd = new_rdd.groupBy(lambda x: x[0])
for key, values in group_by_rdd.collect():
    print("group_by_rdd key: ", key)
    print("group_by_rdd value: ", list(values))

'''
group_by_rdd key:  Fruits
group_by_rdd value:  [('Fruits', 2), ('Fruits', 4)]

group_by_rdd key:  Groceries
group_by_rdd value:  [('Groceries', 3)]

group_by_rdd key:  Cake
group_by_rdd value:  [('Cake', 1), ('Cake', 6)]
'''
sorted_rdd = new_rdd.sortByKey()
print("sorted_rdd: ", sorted_rdd.collect())
#sorted_rdd:  [('Cake', 1), ('Cake', 6), ('Fruits', 2), ('Fruits', 4), ('Groceries', 3)]
