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

input_tuple_data = [("Cake", 1), ("Fruits", 2), ("Groceries", 3), ("Fruits", 4), ("Cake", 6), ("Cake", 1), ("Fruits", 4)]
new_rdd = sc.parallelize(input_tuple_data)

count_by_key_rdd = new_rdd.countByKey()

for k, v in count_by_key_rdd.items():
    print("count_by_key_rdd: Key:", k)
    print("count_by_key_rdd: value:", v)
'''
count_by_key_rdd: Key: Cake
count_by_key_rdd: value: 2
count_by_key_rdd: Key: Fruits
count_by_key_rdd: value: 2
count_by_key_rdd: Key: Groceries
count_by_key_rdd: value: 1
'''
count_by_value_rdd = new_rdd.countByValue()

for k, v in count_by_value_rdd.items():
    print("count_by_value_rdd: Key:", k)
    print("count_by_value_rdd: value:", v)

'''
count_by_value_rdd: Key: ('Cake', 1)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Fruits', 2)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Groceries', 3)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Fruits', 4)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Cake', 6)
count_by_value_rdd: value: 1
'''

'''
count_by_value_rdd: Key: ('Cake', 1)
count_by_value_rdd: value: 2

count_by_value_rdd: Key: ('Fruits', 2)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Groceries', 3)
count_by_value_rdd: value: 1

count_by_value_rdd: Key: ('Fruits', 4)
count_by_value_rdd: value: 2

count_by_value_rdd: Key: ('Cake', 6)
count_by_value_rdd: value: 1
'''

#input_tuple_data = [("Cake", 1), ("Fruits", 2), ("Groceries", 3), ("Fruits", 4), ("Cake", 6), ("Cake", 1), ("Fruits", 4)]

collect_map_rdd = new_rdd.collectAsMap()
for k, v in collect_map_rdd.items():
    print("collect_map_rdd: key: ",k)
    print("collect_map_rdd: value: ",v)

'''
collect_map_rdd: key:  Cake
collect_map_rdd: value:  1

collect_map_rdd: key:  Fruits
collect_map_rdd: value:  4

collect_map_rdd: key:  Groceries
collect_map_rdd: value:  3
'''
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
sorted_rdd_asc = new_rdd.sortByKey()
print("sorted_rdd_asc: ", sorted_rdd_asc.collect())
#sorted_rdd:  [('Cake', 1), ('Cake', 6), ('Fruits', 2), ('Fruits', 4), ('Groceries', 3)]

sorted_rdd_desc = new_rdd.sortByKey(ascending=False)
print("sorted_rdd_desc: ", sorted_rdd_desc.collect())
#sorted_rdd_desc:  [('Groceries', 3), ('Fruits', 2), ('Fruits', 4), ('Cake', 1), ('Cake', 6)]

data = [1,2,3,4,5]
original_rdd = sc.parallelize(data)
print("original_rdd: ", original_rdd.getNumPartitions())
#original_rdd:  2

new_rdd_add_partitions = sc.parallelize(data, 4)
print("new_rdd_add_partitions", new_rdd_add_partitions.getNumPartitions())
#new_rdd_add_partitions 4
