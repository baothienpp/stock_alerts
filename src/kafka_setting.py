KAFKA_TOPIC = 'indicators'
KAFKA_GROUP_ID = 'indicator'

KAFKA_MESSAGE = {'table': '60m', 'symbols': [], 'period': 100, 'mode': 'full'}


class KafkaMessage:
    def __init__(self, table, symbols, period, mode):
        self.table = table
        self.symbols = symbols
        self.period = period
        self.mode = mode

    def to_dict(self):
        return self.__dict__
