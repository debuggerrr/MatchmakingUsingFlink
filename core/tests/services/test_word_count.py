import unittest
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import Types

class TestWordCount(unittest.TestCase):
    def test_word_count(self):
        # Set up the PyFlink streaming environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        env.set_parallelism(1)

        # Set up the input data
        input_data = ["ABC is a good boy. ABC works for an XYZ org"]

        # Create an instance of the WordCount class
        word_counter = WordCount(env)

        # Call the word_count method with the input data
        word_counter.word_count(input_data)

        # Define the expected output
        expected_output = [
            ('abc', 2),
            ('an', 1),
            ('for', 1),
            ('good', 1),
            ('is', 1),
            ('org', 1),
            ('works', 1),
            ('boy.', 1),
            ('xyz', 1)
        ]

        # Create a source for the input data
        source = ListSource(input_data, record_type=Types.STRING())

        # Create a sink to collect the output
        actual_output = []
        def collect_output(x):
            actual_output.append(x)
        sink = env \
            .from_elements([], output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .process(collect_output)

        # Connect the source to the word_count transformation and the sink
        source \
            .flat_map(lambda x, c: c.collect(x.lower().split())) \
            .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .key_by(lambda i: i[0]) \
            .reduce(lambda i, j: (i[0], i[1] + j[1])) \
            .add_sink(sink)

        # Execute the program
        env.execute("test_word_count")

        # Verify that the output is correct
        assert_equals_lists_without_order(expected_output, actual_output)
