from pyspark import SparkContext

sc = SparkContext("local[2]", "Count_Example")

input_tuple_data = [("Cake", 1), ("Fruits", 2), ("Groceries", 3), ("Fruits", 4), ("Cake", 6), ("Cake", 1), ("Fruits", 4)]
new_rdd = sc.parallelize(input_tuple_data)

count_by_key_rdd = new_rdd.countByKey()

for k, v in count_by_key_rdd.items():
    print("count_by_key_rdd: Key:", k)
    print("count_by_key_rdd: value:", v)
    print("--")
print("-------------------------------------------")
count_by_value_rdd = new_rdd.countByValue()

for k, v in count_by_value_rdd.items():
    print("count_by_value_rdd: Key:", k)
    print("count_by_value_rdd: value:", v)
    print("--")

#-------------------------------------------
input_tuple_data = [("Cake", 1), ("Fruits", 2), ("Groceries", 3), ("Fruits", 4), ("Cake", 6), ("Cake", 1), ("Fruits", 4)]

# count_by_value_rdd: Key: ('Cake', 1)
# count_by_value_rdd: value: 2
# --
# count_by_value_rdd: Key: ('Fruits', 2)
# count_by_value_rdd: value: 1
# --
# count_by_value_rdd: Key: ('Groceries', 3)
# count_by_value_rdd: value: 1
# --
# count_by_value_rdd: Key: ('Fruits', 4)
# count_by_value_rdd: value: 2
# --
# count_by_value_rdd: Key: ('Cake', 6)
# count_by_value_rdd: value: 1
# --

sc.stop()