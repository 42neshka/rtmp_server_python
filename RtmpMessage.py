class RTMPMessage:
    # Константы типов сообщений RTMP
    AUDIO = 8
    VIDEO = 9
    DATA = 18

    def __init__(self, packet):
        self.streamId = packet['header']['stream_id']
        self.type = packet['header']['type']
        self.data = packet['payload']
        self.size = packet['header']['length']
        self.time = packet['header']['timestamp']
