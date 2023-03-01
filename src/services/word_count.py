from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import Types


class WordCount(object):
    def __init__(self, env):
        self.env = env
        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.env.set_parallelism(1)

    def word_count(self, input_data):
        text = self.env.from_collection(input_data)
        word_count = text.flat_map(lambda x: str(x).lower().split()) \
            .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
            .key_by(lambda i: i[0]) \
            .reduce(lambda i, j: (i[0], i[1] + j[1]))
        word_count.print()
        self.env.execute("some-name")




WordCount(StreamExecutionEnvironment.get_execution_environment()).word_count(["ABC is a good boy. ABC works for an XYZ org"])
