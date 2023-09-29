from src.main.utilities.SparkContextHandler import create_spark_context

def read_text_file(input_path):
    input_rdd = sc.textFile(input_path)
    return input_rdd

sc = create_spark_context("local[4]", "WordCountExample")
words_input_path = "..\\resources\\input\\wordcount_input.txt"
word_rdd = read_text_file(words_input_path)
#print("word_rdd: ", word_rdd.collect())
#word_rdd:  ['Hi I am Learning PySpark. I want to become a Data Engineer.', 'I am learning new skills to improve my career path step by step.', 'Thank you very much.']

#split_rdd = word_rdd.map(lambda line: line.split(" "))
#print("split_rdd: ", split_rdd.collect())
#split_rdd:  [['Hi', 'I', 'am', 'Learning', 'PySpark.', 'I', 'want', 'to', 'become', 'a', 'Data', 'Engineer.'], ['I', 'am', 'learning', 'new', 'skills', 'to', 'improve', 'my', 'career', 'path', 'step', 'by', 'step.'], ['Thank', 'you', 'very', 'much.']]

split_flatmap_rdd = word_rdd.flatMap(lambda l: l.split(" "))
#print("split_flatmap_rdd: ", split_flatmap_rdd.collect())
#split_flatmap_rdd:  ['Hi', 'I', 'am', 'Learning', 'PySpark.', 'I', 'want', 'to', 'become', 'a', 'Data', 'Engineer.', 'I', 'am', 'learning', 'new', 'skills', 'to', 'improve', 'my', 'career', 'path', 'step', 'by', 'step.', 'Thank', 'you', 'very', 'much.']

word_number_assigned_rdd = split_flatmap_rdd.map(lambda word: (word, 1))
print("word_number_assigned_rdd: ", word_number_assigned_rdd.collect())
#word_number_assigned_rdd:  [('Hi', 1), ('I', 1), ('am', 1), ('Learning', 1), ('PySpark.', 1), ('I', 1), ('want', 1), ('to', 1), ('become', 1), ('a', 1), ('Data', 1), ('Engineer.', 1), ('I', 1), ('am', 1), ('learning', 1), ('new', 1), ('skills', 1), ('to', 1), ('improve', 1), ('my', 1), ('career', 1), ('path', 1), ('step', 1), ('by', 1), ('step.', 1), ('Thank', 1), ('you', 1), ('very', 1), ('much.', 1)]

aggregated_words_rdd = word_number_assigned_rdd.reduceByKey(lambda x, y: x + y)
#print("aggregated_words_rdd: ", aggregated_words_rdd.collect())
#aggregated_words_rdd:  [('Hi', 1), ('am', 2), ('become', 1), ('Engineer.', 1), ('learning', 1), ('new', 1), ('improve', 1), ('career', 1), ('path', 1), ('step', 1), ('step.', 1), ('Thank', 1), ('very', 1), ('much.', 1), ('I', 3), ('Learning', 1), ('PySpark.', 1), ('want', 1), ('to', 2), ('a', 1), ('Data', 1), ('skills', 1), ('my', 1), ('by', 1), ('you', 1)]

desc_order_words_rdd = aggregated_words_rdd.sortBy(lambda x: x[1], ascending=False)
print("desc_order_words_rdd: ", desc_order_words_rdd.collect())
#desc_order_words_rdd:  [('I', 3), ('am', 2), ('to', 2), ('Hi', 1), ('become', 1), ('Engineer.', 1), ('learning', 1), ('new', 1), ('improve', 1), ('career', 1), ('path', 1), ('step', 1), ('step.', 1), ('Thank', 1), ('very', 1), ('much.', 1), ('Learning', 1), ('PySpark.', 1), ('want', 1), ('a', 1), ('Data', 1), ('skills', 1), ('my', 1), ('by', 1), ('you', 1)]
