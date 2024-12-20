import asyncio
import logging
import amf
import av
import common
import struct
from typing import Optional
import time
import handshake
import uuid

from RtmpMessage import RTMPMessage

# Config
LogLevel = logging.INFO

# RTMP packet types
RTMP_TYPE_SET_CHUNK_SIZE = 1  # Set Chunk Size message (RTMP_PACKET_TYPE_CHUNK_SIZE 0x01) - The Set Chunk Size message is used to inform the peer about the chunk size for subsequent chunks.
RTMP_TYPE_ABORT = 2  # Abort message - The Abort message is used to notify the peer to discard a partially received message.
RTMP_TYPE_ACKNOWLEDGEMENT = 3  # Acknowledgement message (RTMP_PACKET_TYPE_BYTES_READ_REPORT 0x03) - The Acknowledgement message is used to report the number of bytes received so far.
RTMP_PACKET_TYPE_CONTROL = 4  # Control message - Control messages carry protocol control information between the RTMP peers.
RTMP_TYPE_WINDOW_ACKNOWLEDGEMENT_SIZE = 5  # Window Acknowledgement Size message (RTMP_PACKET_TYPE_SERVER_BW 0x05) - The Window Acknowledgement Size message is used to inform the peer about the window acknowledgement size.
RTMP_TYPE_SET_PEER_BANDWIDTH = 6  # Set Peer Bandwidth message (RTMP_PACKET_TYPE_CLIENT_BW 0x06) - The Set Peer Bandwidth message is used to inform the peer about the available outgoing bandwidth.
RTMP_TYPE_AUDIO = 8  # Audio data message (RTMP_PACKET_TYPE_AUDIO 0x08) - The Audio data message carries audio data.
RTMP_TYPE_VIDEO = 9  # Video data message (RTMP_PACKET_TYPE_VIDEO 0x09) - The Video data message carries video data.
RTMP_TYPE_FLEX_STREAM = 15  # Flex Stream message (RTMP_PACKET_TYPE_FLEX_STREAM_SEND 0x0F) - The Flex Stream message is used to send AMF3-encoded stream metadata.
RTMP_TYPE_FLEX_OBJECT = 16  # Flex Shared Object message (RTMP_PACKET_TYPE_FLEX_SHARED_OBJECT 0x10) - The Flex Shared Object message is used to send AMF3-encoded shared object data.
RTMP_TYPE_FLEX_MESSAGE = 17  # Flex Message message (RTMP_PACKET_TYPE_FLEX_MESSAGE 0x11) - The Flex Message message is used to send AMF3-encoded RPC or shared object events.
RTMP_TYPE_DATA = 18  # AMF0 Data message (RTMP_PACKET_TYPE_INFO 0x12) - The AMF0 Data message carries generic AMF0-encoded data.
RTMP_TYPE_SHARED_OBJECT = 19  # AMF0 Shared Object message (RTMP_PACKET_TYPE_INFO 0x12) - The AMF0 Shared Object message carries AMF0-encoded shared object data.
RTMP_TYPE_INVOKE = 20  # AMF0 Invoke message (RTMP_PACKET_TYPE_SHARED_OBJECT 0x13) - The AMF0 Invoke message is used for remote procedure calls (RPC) or command execution.
RTMP_TYPE_METADATA = 22  # Metadata message (RTMP_PACKET_TYPE_FLASH_VIDEO 0x16) - The Metadata message carries metadata related to the media stream.

RTMP_CHUNK_TYPE_0 = 0  # 11-bytes: timestamp(3) + length(3) + stream type(1) + stream id(4)
RTMP_CHUNK_TYPE_1 = 1  # 7-bytes: delta(3) + length(3) + stream type(1)
RTMP_CHUNK_TYPE_2 = 2  # 3-bytes: delta(3)
RTMP_CHUNK_TYPE_3 = 3  # 0-byte

# RTMP channel constants
RTMP_CHANNEL_PROTOCOL = 2
RTMP_CHANNEL_INVOKE = 3
RTMP_CHANNEL_AUDIO = 4
RTMP_CHANNEL_VIDEO = 5
RTMP_CHANNEL_DATA = 6

# Protocol channel ID
PROTOCOL_CHANNEL_ID = 2

MAX_CHUNK_SIZE = 10485760

# Constants for Packet Types
PacketTypeSequenceStart = 0  # Represents the start of a video/audio sequence
PacketTypeCodedFrames = 1  # Represents a video/audio frame
PacketTypeSequenceEnd = 2  # Represents the end of a video/audio sequence
PacketTypeCodedFramesX = 3  # Represents an extended video/audio frame
PacketTypeMetadata = 4  # Represents a packet with metadata
PacketTypeMPEG2TSSequenceStart = 5  # Represents the start of an MPEG2-TS video/audio sequence

# Constants for FourCC values
FourCC_AV1 = b'av01'  # AV1 video codec
FourCC_VP9 = b'vp09'  # VP9 video codec
FourCC_HEVC = b'hvc1'  # HEVC video codec

# Dictionary to store live users
LiveUsers = {}
# Dictionary to store player users
PlayerUsers = {}


# Custom exception for disconnecting clients
class DisconnectClientException(Exception):
    pass


# Class representing the state of a connected client
class ClientState:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.client_ip = '0.0.0.0'

        # RTMP properties
        self.chunk_size = 128  # Default chunk size
        self.out_chunk_size = 4096  # Default out chunk size
        self.window_acknowledgement_size = 5000000  # Default window acknowledgement size
        self.peer_bandwidth = 0  # Default peer bandwidth

        # RTMP Invoke Connect Data
        self.flashVer = 'FMLE/3.0 (compatible; FMSc/1.0)'
        self.connectType = 'nonprivate'
        self.tcUrl = ''
        self.swfUrl = ''
        self.app = ''
        self.objectEncoding = 0

        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

        self.lastWriteHeaders = dict()
        self.nextChannelId = PROTOCOL_CHANNEL_ID + 1
        self.streams = 0
        self._time0 = time.time()
        self.stream_mode = None

        self.streamPath = ''
        self.publishStreamId = 0
        self.publishStreamPath = ''
        self.CacheState = 0
        self.IncomingPackets = {}
        self.Players = {}

        # Meta Data
        self.metaData = None
        self.metaDataPayload = None
        self.audioSampleRate = 0
        self.audioChannels = 1
        self.videoWidth = 0
        self.videoHeight = 0
        self.videoFps = 0
        self.Bitrate = 0

        self.isFirstAudioReceived = False
        self.isReceiveVideo = False
        self.aacSequenceHeader = None
        self.avcSequenceHeader = None
        self.audioCodec = 0
        self.audioCodecName = ''
        self.audioProfileName = ''
        self.videoCodec = 0
        self.videoCodecName = ''
        self.videoProfileName = ''
        self.videoCount = 0
        self.videoLevel = 0

        self.inAckSize = 0
        self.inLastAck = 0


# RTMP server class
class RTMPServer:
    def __init__(self, host='127.0.0.1', port=1935):
        # Socket
        # Server socket properties
        self.host = host
        self.port = port
        self.client_states = {}

        self.logger = logging.getLogger('RTMPServer')
        self.logger.setLevel(LogLevel)

    async def handle_client(self, reader, writer):
        # Create a new client state for each connected client
        client_state = ClientState()
        self.client_states[client_state.id] = client_state
        self.client_states[client_state.id].clientID = client_state.id

        self.client_states[client_state.id].reader = reader
        self.client_states[client_state.id].writer = writer

        self.client_states[client_state.id].client_ip = writer.get_extra_info('peername')
        self.logger.info("New client connected: %s", self.client_states[client_state.id].client_ip)

        # Perform RTMP handshake
        try:
            await asyncio.wait_for(self.perform_handshake(client_state.id), timeout=5)
        except asyncio.TimeoutError:
            self.logger.error("Handshake timeout. Closing connection: %s",
                              self.client_states[client_state.id].client_ip)
            await self.disconnect(client_state.id)
            return

        # Process RTMP messages
        while True:
            try:
                await self.get_chunk_data(client_state.id)

            except asyncio.TimeoutError:
                self.logger.debug("Connection timeout. Closing connection: %s",
                                  self.client_states[client_state.id].client_ip)
                break

            except DisconnectClientException:
                self.logger.debug("Disconnecting client: %s", self.client_states[client_state.id].client_ip)
                break

            except ConnectionAbortedError as e:
                self.logger.debug("Connection aborted by client: %s", self.client_states[client_state.id].client_ip)
                break

            except Exception as e:
                self.logger.error("An error occurred: %s", str(e))
                break

        await self.disconnect(client_state.id)

    async def disconnect(self, client_id):
        client_state = self.client_states.get(client_id)

        # Удаляем из LiveUsers, если клиент является издателем
        if client_state and client_state.app in LiveUsers:
            if LiveUsers[client_state.app].get('client_id') == client_id:
                self.logger.info("Disconnecting publisher for app: %s", client_state.app)
                del LiveUsers[client_state.app]

        # Удаляем из Players, если клиент является плеером
        if client_state and client_state.app in LiveUsers:
            publisher_id = LiveUsers[client_state.app].get('client_id')
            if publisher_id:
                publisher_client_state = self.client_states.get(publisher_id)
                if hasattr(publisher_client_state, "Players") and client_id in publisher_client_state.Players:
                    del publisher_client_state.Players[client_id]
                    self.logger.info("Player removed from Players: %s", client_id)
            self.logger.info("PlayerUsers state: %s", client_state.Players)

        # Удаляем из PlayerUsers
        if client_state and client_state.app in PlayerUsers:
            if client_id in PlayerUsers[client_state.app]:
                del PlayerUsers[client_state.app][client_id]
                self.logger.info("Player removed from PlayerUsers: %s", client_id)
            self.logger.info("PlayerUsers state: %s", PlayerUsers)
            # Если больше нет игроков для данного приложения, удаляем ключ
            if not PlayerUsers[client_state.app]:
                del PlayerUsers[client_state.app]

        # Удаляем состояние клиента
        if client_id in self.client_states:
            del self.client_states[client_id]
            self.logger.info("Client state removed for client: %s", client_id)

    async def get_chunk_data(self, client_id):
        # Read a chunk of data from the client

        client_state = self.client_states[client_id]
        payload_length = 0

        try:
            chunk_data = await client_state.reader.readexactly(1)
            if not chunk_data:
                raise DisconnectClientException()

            cid = chunk_data[0] & 0b00111111

            # Chunk Basic Header field may be 1, 2, or 3 bytes, depending on the chunk stream ID.
            if cid == 0:  # ChunkBasicHeader: 2
                chunk_data += await client_state.reader.readexactly(1)  # Need read 1 more packet
                cid = 64 + chunk_data[1]  # Chunk stream IDs 64-319 can be encoded in the 2-byte form of the header
            elif cid == 1:  # ChunkBasicHeader: 3
                chunk_data += await client_state.reader.readexactly(2)  # Need read 2 more packets
                cid = (64 + chunk_data[1] + chunk_data[
                    2]) << 8  # Chunk stream IDs 64-65599 can be encoded in the 3-byte version of this field

            chunk_full = bytearray(chunk_data)
            fmt = (chunk_data[0] & 0b11000000) >> 6

            if not cid in client_state.IncomingPackets:
                client_state.IncomingPackets[cid] = self.createPacket(cid, fmt)

            # I'm afraid I suffer from memory leaks. :D
            client_state.IncomingPackets[cid]['last_received_time'] = time.time()
            self.clearPayloadIfTimeout(client_id, 120)

            header_data = bytearray()
            # Get Message Timestamp for FMT 0, 1, 2
            if fmt <= RTMP_CHUNK_TYPE_2:
                timestamp_bytes = await client_state.reader.readexactly(3)
                header_data += timestamp_bytes
                client_state.IncomingPackets[cid]['timestamp'] = int.from_bytes(timestamp_bytes, byteorder='big')
                del timestamp_bytes

            # Get Message Length and Message Type for FMT 0, 1
            if fmt <= RTMP_CHUNK_TYPE_1:
                length_bytes = await client_state.reader.readexactly(3)
                header_data += length_bytes
                type_bytes = await client_state.reader.readexactly(1)
                header_data += type_bytes
                client_state.IncomingPackets[cid]['payload_length'] = int.from_bytes(length_bytes, byteorder='big')
                client_state.IncomingPackets[cid]['msg_type_id'] = int.from_bytes(type_bytes, byteorder='big')
                client_state.IncomingPackets[cid]['payload'] = bytearray()
                del length_bytes
                del type_bytes

            # Get Message Stream ID for FMT 0
            if fmt == RTMP_CHUNK_TYPE_0:
                streamID_bytes = await client_state.reader.readexactly(4)
                header_data += streamID_bytes
                client_state.IncomingPackets[cid]['msg_stream_id'] = int.from_bytes(streamID_bytes, byteorder='big')
                del streamID_bytes

            chunk_full += header_data

            # Set Main Packet Headers and payload_length for FMT 0, 1
            if fmt <= RTMP_CHUNK_TYPE_1:
                # client_state.IncomingPackets[cid]['basic_header'] = chunk_data
                # client_state.IncomingPackets[cid]['header'] = header_data
                payload_length = client_state.IncomingPackets[cid]['payload_length']

            # Calculate Payload Remaining length for FMT 2,3 
            if fmt > RTMP_CHUNK_TYPE_1:
                payload_length = client_state.IncomingPackets[cid]['payload_length'] - len(
                    client_state.IncomingPackets[cid]['payload'])

            # Check message type id
            if RTMP_TYPE_METADATA < client_state.IncomingPackets[cid]['msg_type_id']:
                self.logger.error("Invalid Packet Type: %s", str(client_state.IncomingPackets[cid]['msg_type_id']))
                raise DisconnectClientException()

            # Messages with type=3 should never have ext timestamp field according to standard. However that's not always the case in real life
            if client_state.IncomingPackets[cid][
                'timestamp'] == 0xffffff:  # Max Value check (16777215), Need to read extended timestamp
                extended_timestamp_bytes = await client_state.reader.readexactly(4)
                chunk_full += extended_timestamp_bytes
                client_state.IncomingPackets[cid]['extended_timestamp'] = int.from_bytes(extended_timestamp_bytes,
                                                                                         byteorder='big')
                del extended_timestamp_bytes

            client_state.inAckSize += len(chunk_full)

            self.logger.debug(
                f"FMT: {fmt}, CID: {cid}, Message Length: {payload_length}, Timestamp: {client_state.IncomingPackets[cid]['timestamp']}")

            if payload_length > 0:
                payload_length = min(client_state.chunk_size, payload_length)
                payload = await client_state.reader.readexactly(payload_length)
                client_state.inAckSize += len(payload)
                client_state.IncomingPackets[cid]['payload'] += payload
                del payload
            else:
                # I'm not sure. In some cases, I may need to disconnect the client, while in other cases, I won't. I will ignore the issue and proceed to the next packet, but I will clear the payload. If invalid data continues, it may result in a disconnection when processing subsequent packets.
                self.logger.error(
                    f"Invalid Length (ZERO!), FMT: {fmt}, CID: {cid}, Message Length: {payload_length}, Timestamp: {client_state.IncomingPackets[cid]['timestamp']}")
                client_state.IncomingPackets[cid]['payload'] = bytearray()
                return

            if client_state.inAckSize >= 0xF0000000:
                client_state.inAckSize = 0
                client_state.inLastAck = 0

            # Delete some variables for fun!
            del chunk_data
            del chunk_full
            del payload_length
            del header_data

            if len(client_state.IncomingPackets[cid]['payload']) >= client_state.IncomingPackets[cid]['payload_length']:
                rtmp_packet = {
                    "header": {
                        "fmt": client_state.IncomingPackets[cid]["fmt"],
                        "cid": client_state.IncomingPackets[cid]["cid"],
                        "timestamp": client_state.IncomingPackets[cid]["timestamp"],
                        "length": client_state.IncomingPackets[cid]["payload_length"],
                        "type": client_state.IncomingPackets[cid]["msg_type_id"],
                        "stream_id": client_state.IncomingPackets[cid]["msg_stream_id"]
                    },
                    "clock": 0,
                    "payload": client_state.IncomingPackets[cid]['payload']
                }
                client_state.IncomingPackets[cid]['payload'] = bytearray()
                await self.handle_rtmp_packet(client_id, rtmp_packet)
                del rtmp_packet

            # Send ACK If needed!
            if (
                    client_state.window_acknowledgement_size > 0 and client_state.inAckSize - client_state.inLastAck >= client_state.window_acknowledgement_size):
                client_state.inLastAck = client_state.inAckSize
                await self.send_ack(client_id, client_state.inAckSize)

        except Exception as e:
            self.logger.error("An error occurred: %s", str(e))
            raise DisconnectClientException()

    # This function is designed to safely stop memory leaks if they exist. It ensures that memory is properly managed and prevents any potential leaks from causing issues.
    def clearPayloadIfTimeout(self, client_id, packet_timeout=30):
        client_state = self.client_states[client_id]
        current_time = time.time()
        for cid, packet in client_state.IncomingPackets.items():
            if 'last_received_time' in packet and current_time - packet['last_received_time'] >= packet_timeout:
                packet['payload'] = bytearray()  # Clear the payload

    def createPacket(self, cid, fmt):
        out = {}
        out['fmt'] = fmt
        out['cid'] = cid
        # out['basic_header'] = bytearray()
        # out['header'] = bytearray()

        out['timestamp'] = 0
        out['extended_timestamp'] = 0
        out['payload_length'] = 0
        out['msg_type_id'] = 0
        out['msg_stream_id'] = 0
        out['payload'] = bytearray()
        out['last_received_time'] = time.time()

        return out

    async def perform_handshake(self, client_id):
        # Perform the RTMP handshake with the client
        client_state = self.client_states[client_id]

        c0_data = await client_state.reader.readexactly(1)
        if c0_data != bytes([0x03]) and c0_data != bytes([0x06]):
            client_state.writer.close()
            await client_state.writer.wait_closed()
            # self.logger.info("Invalid Handshake, Client disconnected: %s", self.client_ip)

        c1_data = await client_state.reader.readexactly(1536)
        clientType = bytes([3])
        messageFormat = handshake.detectClientMessageFormat(c1_data)
        if messageFormat == handshake.MESSAGE_FORMAT_0:
            await self.send(client_id, clientType)
            s1_data = c1_data
            s2_data = c1_data
            await self.send(client_id, c1_data)
            await client_state.reader.readexactly(len(s1_data))
            await self.send(client_id, s2_data)
        else:
            s1_data = handshake.generateS1(messageFormat)
            s2_data = handshake.generateS2(messageFormat, c1_data)
            data = clientType + s1_data + s2_data
            client_state.writer.write(data)
            s1_data = await client_state.reader.readexactly(len(s1_data))

        self.logger.debug("Handshake done!")

    async def handle_rtmp_packet(self, client_id, rtmp_packet):
        """
        Эта главная функция, которая определяет тип пакета и направляет на соответствующий обработчик
        """
        # Handle an RTMP packet from the client
        # client_state = self.client_states[client_id]

        # Extract information from rtmp_packet and process as needed
        msg_type_id = rtmp_packet["header"]["type"]
        payload = rtmp_packet["payload"]

        self.logger.debug("Received RTMP packet: type=%s, payload=%s", msg_type_id, payload)

        # self.logger.debug("Received RTMP packet:")
        # self.logger.debug("  RTMP Packet Type: %s", msg_type_id)

        if msg_type_id == RTMP_TYPE_SET_CHUNK_SIZE:
            self.handle_chunk_size_message(client_id, payload)
        elif msg_type_id == RTMP_TYPE_ACKNOWLEDGEMENT:
            await self.handle_bytes_read_report(client_id, payload)
        # elif msg_type_id == RTMP_PACKET_TYPE_CONTROL:
        #     self.handle_control_message(payload)
        elif msg_type_id == RTMP_TYPE_WINDOW_ACKNOWLEDGEMENT_SIZE:
            self.handle_window_acknowledgement_size(client_id, payload)
        elif msg_type_id == RTMP_TYPE_SET_PEER_BANDWIDTH:
            self.handle_set_peer_bandwidth(client_id, payload)
        elif msg_type_id == RTMP_TYPE_AUDIO:
            await self.handle_audio_data(client_id, rtmp_packet)
        elif msg_type_id == RTMP_TYPE_VIDEO:
            await self.handle_video_data(client_id, rtmp_packet)
        # elif msg_type_id == RTMP_TYPE_FLEX_STREAM:
        #     self.handle_flex_stream_message(payload)
        # elif msg_type_id == RTMP_TYPE_FLEX_OBJECT:
        #     self.handle_flex_shared_object_message(payload)
        elif msg_type_id == RTMP_TYPE_FLEX_MESSAGE:
            invoke_message = self.parse_amf0_invoke_message(rtmp_packet)

            self.logger.info("Processing RTMP_TYPE_INVOKE packet.")
            await self.handle_invoke_message(client_id, invoke_message)
        elif msg_type_id == RTMP_TYPE_DATA:
            await self.handle_amf_data(client_id, rtmp_packet)
        # elif msg_type_id == RTMP_TYPE_SHARED_OBJECT:
        #     self.handle_amf0_shared_object_message(payload)
        elif msg_type_id == RTMP_TYPE_INVOKE:
            invoke_message = self.parse_amf0_invoke_message(rtmp_packet)

            self.logger.info("Processing RTMP_TYPE_INVOKE packet.")
            await self.handle_invoke_message(client_id, invoke_message)
        # elif msg_type_id == RTMP_TYPE_METADATA:
        #     self.handle_metadata_message(payload)
        else:
            self.logger.info("Unsupported RTMP packet type: %s", msg_type_id)

    async def handle_video_data(self, client_id, rtmp_packet):
        # Handle video data in an RTMP packet
        client_state = self.client_states[client_id]
        payload = rtmp_packet['payload']

        # Проверяем, есть ли плееры
        # if client_state.app in PlayerUsers and PlayerUsers[client_state.app]:
        #     for player_id, player_state in PlayerUsers[client_state.app].items():
        #         if isinstance(player_state, ClientState):
        #             self.logger.info("Forwarding video packet to player: %s", player_id)
        #             try:
        #                 # Преобразование rtmp_packet
        #                 message = RTMPMessage(rtmp_packet)
        #                 await self.writeMessage(player_id, message)
        #                 self.logger.info(
        #                     f"SENT VIDEO PACKET TO PLAYER {client_id}, streamId: {message.streamId}, size: {message.size}, type: {message.type}")
        #                 if client_state.avcSequenceHeader:
        #                     self.logger.info(f"Sending AVC Sequence Header to player: {player_id}")
        #
        #             except Exception as e:
        #                 self.logger.error("Failed to send video packet to player %s: %s", player_id, e)
        #         else:
        #             self.logger.error("Invalid player state for player_id %s: %s", player_id, type(player_state))
        # else:
        #     self.logger.info("No players connected to receive video packets.")

        isExHeader = (payload[0] >> 4 & 0b1000) != 0
        self.logger.info(f"ISEXHEADER PAYLOAD: {payload[0] >> 4 & 0b1000}")
        self.logger.info(f"ISEXHEADER PAYLOAD: {payload[0]:08b}, ISEXHEADER: {isExHeader}")
        self.logger.info(f"PAYLOAD: {payload[:50]}")
        frame_type = payload[0] >> 4 & 0b0111
        codec_id = payload[0] & 0x0f
        packetType = payload[0] & 0x0f

        # Handle Video Data!
        self.logger.info(f"ISEXHEADER: {isExHeader}")
        if isExHeader:
            if packetType == PacketTypeMetadata:
                pass
            elif packetType == PacketTypeSequenceEnd:
                pass

            FourCC = payload[1:5]
            if FourCC == FourCC_HEVC:
                codec_id = 12
                if packetType == PacketTypeSequenceStart:
                    payload[0] = 0x1c
                    payload[1:5] = b'\x00\x00\x00\x00'
                elif packetType in [PacketTypeCodedFrames, PacketTypeCodedFramesX]:
                    if packetType == PacketTypeCodedFrames:
                        payload = payload[3:]
                    else:
                        payload[2:5] = b'\x00\x00\x00'
                    payload[0] = (frame_type << 4) | 0x0c
                    payload[1] = 1
                    self.logger.info(f"PAYLOAD: {payload[:50]}")
            elif FourCC == FourCC_AV1:
                codec_id = 13
                if packetType == PacketTypeSequenceStart:
                    payload[0] = 0x1d
                    payload[1:5] = b'\x00\x00\x00\x00'
                elif packetType == PacketTypeMPEG2TSSequenceStart:
                    pass
                elif packetType == PacketTypeCodedFrames:
                    payload[0] = (frame_type << 4) | 0x0d
                    payload[1] = 1
                    payload[2:5] = b'\x00\x00\x00'
            else:
                self.logger.debug("unsupported extension header")
                return

        # self.logger.info(f"PAYLOAD: {payload[0]}")
        # self.logger.info(f"PAYLOAD: {payload[0]:08b}")
        # self.logger.info(f"PAYLOAD: {payload}")

        if codec_id in [7, 12, 13]:
            self.logger.info(f"CODEC_ID: {codec_id}")
            self.logger.info(f"PAYLOAD[1]: {payload[1]}")

            if frame_type == 1: # and payload[1] == 0:  # I-frame
                self.logger.info("Processing I-frame")
                # self.logger.info(f"I-frame payload: {payload.hex()}")
                client_state.avcSequenceHeader = bytearray(payload)
                self.logger.info(f"AVCSEQUENCEHEADER: {client_state.avcSequenceHeader[:100]}")
                # self.logger.info(f"Payload data before parsing: {client_state.avcSequenceHeader.hex()}")

                try:
                    if payload[0] & 0x1f == 7:  # NAL Type 7 (SPS)
                        self.logger.info("Valid SPS detected")
                    else:
                        self.logger.error("Invalid SPS or missing")


                    info = av.readAVCSpecificConfig(client_state.avcSequenceHeader)
                    self.logger.info(f"AVC Info: {info}")
                    client_state.videoWidth = info['width']
                    client_state.videoHeight = info['height']
                    client_state.videoProfileName = av.getAVCProfileName(info)
                    client_state.videoLevel = info['level']
                    self.logger.info("CodecID: %d, Video Level: %f, Profile Name: %s, Width: %d, Height: %d, Profile: %d",
                                 codec_id, client_state.videoLevel, client_state.videoProfileName,
                                 client_state.videoWidth, client_state.videoHeight, info['profile'])
                except KeyError as e:
                    self.logger.error(f"Missing key in AVC Info: {e}")

            elif frame_type == 2:  # P-frame
                self.logger.info("Processing P-frame")
            else:
                self.logger.error(f"FRAME_TYPE_UNKNOWN: {frame_type}")
                self.logger.info(f"CODEC_ID UNKNOWN: {codec_id}")

        # Кодек клиента устанавливается только один раз, когда он равен 0 и больше не меняется (7 = H.264)
        if client_state.videoCodec == 0:
            client_state.videoCodec = codec_id
            client_state.videoCodecName = common.VIDEO_CODEC_NAME[codec_id]
            # self.logger.info(f"CLIENT STATE{client_state.__dict__}")

    async def handle_audio_data(self, client_id, rtmp_packet):
        client_state = self.client_states[client_id]
        payload = rtmp_packet['payload']

        # Проверяем, есть ли плееры
        if client_state.app in PlayerUsers and PlayerUsers[client_state.app]:
            for player_id, player_state in PlayerUsers[client_state.app].items():
                if isinstance(player_state, ClientState):
                    self.logger.info("Forwarding audio packet to player: %s", player_id)
                    try:
                        # Преобразование rtmp_packet
                        message = RTMPMessage(rtmp_packet)
                        await self.writeMessage(player_id, message)
                        self.logger.info(
                            f"SENT AUDIO PACKET TO PLAYER {client_id}, streamId: {message.streamId}, size: {message.size}, type: {message.type}")
                        if client_state.aacSequenceHeader:
                            self.logger.info(f"Sending AAC Sequence Header to player: {player_id}")
                            self.logger.info(f"CLIENT_STATE.AACSEQUENCEHEADER: {client_state.aacSequenceHeader}")
                    except Exception as e:
                        self.logger.error("Failed to send audio packet to player %s: %s", player_id, e)
                else:
                    self.logger.error("Invalid player state for player_id %s: %s", player_id, type(player_state))
        # else:
        #     self.logger.info("No players connected to receive audio packets.")

        # Разбираем аудиопакет
        sound_format = (payload[0] >> 4) & 0x0f
        sound_type = payload[0] & 0x01
        sound_size = (payload[0] >> 1) & 0x01
        sound_rate = (payload[0] >> 2) & 0x03

        if client_state.audioCodec == 0:
            # Инициализируем аудиокодек
            client_state.audioCodec = sound_format
            # Безопасная обработка AUDIO_CODEC_NAME
            client_state.audioCodecName = (
                av.AUDIO_CODEC_NAME[sound_format]
                if 0 <= sound_format < len(av.AUDIO_CODEC_NAME)
                else "Unknown"
            )

            # Безопасная обработка AUDIO_SOUND_RATE
            client_state.audioSampleRate = (
                av.AUDIO_SOUND_RATE[sound_rate]
                if 0 <= sound_rate < len(av.AUDIO_SOUND_RATE)
                else 0
            )
            client_state.audioChannels = sound_type + 1

            # Обработка специфических форматов
            if sound_format == 4:  # Nellymoser 16 kHz
                client_state.audioSampleRate = 16000
            elif sound_format in (5, 7, 8):  # Nellymoser 8 kHz | G.711
                client_state.audioSampleRate = 8000
            elif sound_format == 11:  # Speex
                client_state.audioSampleRate = 16000
            elif sound_format == 14:  # MP3 8 kHz
                client_state.audioSampleRate = 8000

        if (sound_format == 10 or sound_format == 13) and payload[1] == 0:
            # Сохраняем AAC Sequence Header
            client_state.isFirstAudioReceived = True
            client_state.aacSequenceHeader = payload

            if sound_format == 10:  # AAC
                info = av.read_aac_specific_config(client_state.aacSequenceHeader)
                client_state.audioProfileName = av.get_aac_profile_name(info)
                client_state.audioSampleRate = info['sample_rate']
                client_state.audioChannels = info['channels']
            else:  # Прочие форматы
                client_state.audioSampleRate = 48000
                client_state.audioChannels = payload[11]

    def handle_chunk_size_message(self, client_id, payload):
        # Handle Chunk Size message
        new_chunk_size = int.from_bytes(payload, byteorder='big')
        if (MAX_CHUNK_SIZE < new_chunk_size):
            self.logger.debug("Chunk size is too big!", new_chunk_size)
            raise DisconnectClientException()

        self.client_states[client_id].chunk_size = new_chunk_size
        self.logger.debug("Updated chunk size: %d", self.client_states[client_id].chunk_size)

    def handle_window_acknowledgement_size(self, client_id, payload):
        # Handle Window Acknowledgement Size message
        client_state = self.client_states[client_id]
        new_window_acknowledgement_size = int.from_bytes(payload, byteorder='big')
        client_state.window_acknowledgement_size = new_window_acknowledgement_size
        self.logger.debug("Updated window acknowledgement size: %d", client_state.window_acknowledgement_size)

    def handle_set_peer_bandwidth(self, client_id, payload):
        # Handle Set Peer Bandwidth message
        client_state = self.client_states[client_id]
        bandwidth = int.from_bytes(payload[:4], byteorder='big')
        limit_type = payload[4]
        client_state.peer_bandwidth = bandwidth
        self.logger.debug("Updated peer bandwidth: %d, Limit type: %d", client_state.peer_bandwidth, limit_type)


    # Новая версия метода
    async def handle_invoke_message(self, client_id, invoke):
        # Проверяем наличие ключа 'command' перед использованием
        if 'cmd' not in invoke:
            self.logger.error("Invoke message missing 'cmd': %s", invoke)
            raise DisconnectClientException()

        command = invoke['cmd']
        self.logger.info("Invoke command received: %s", command.upper())

        # Обработка команды
        if command == 'connect':
            self.logger.info("Received connect invoke")
            await self.handle_connect_command(client_id, invoke)
        elif command in ['releaseStream', 'FCPublish', 'FCUnpublish', 'getStreamLength']:
            self.logger.info("Received %s invoke", command)
            return
        elif command == 'createStream':
            self.logger.info("Received createStream invoke")
            await self.response_createStream(client_id, invoke)
        elif command == 'publish':
            self.logger.info("Received publish invoke")
            await self.handle_publish(client_id, invoke)
        elif command == 'play':
            self.logger.info("Received play invoke")
            await self.handle_onPlay(client_id, invoke)
        else:
            self.logger.info("Unsupported invoke command: %s", command)

    async def handle_onPlay(self, client_id, invoke):
        # Получение состояния клиента (объекта ClientState), связанного с данным client_id.
        client_state = self.client_states[client_id]

        # Проверяем, существует ли поток
        if client_state.app not in LiveUsers or not LiveUsers[client_state.app]:
            self.logger.warning("No active streams found for app: %s", client_state.app)
            raise DisconnectClientException()

        # Получение идентификатора клиента, который публикует поток (из LiveUsers).
        publisher_id = LiveUsers[client_state.app]['client_id']
        # Извлечение состояния клиента, который публикует поток, на основе его client_id.
        publisher_client_state = self.client_states[publisher_id]
        self.logger.info(f"PUBLISHER_CLIENT_STATE {publisher_client_state}")

        if not publisher_id:
            self.logger.warning("No publisher found for app: %s", client_state.app)
            raise DisconnectClientException()

        # Добавляем клиента в PlayerUsers
        if client_state.app not in PlayerUsers:
            PlayerUsers[client_state.app] = {}

        if client_id not in PlayerUsers[client_state.app]:
            PlayerUsers[client_state.app][client_id] = client_state  # Сохраняем объект ClientState
            self.logger.info("Player added to PlayerUsers: %s", PlayerUsers[client_state.app])

        # Инициализируем Players, если еще не создано
        if not hasattr(publisher_client_state, "Players"):
            publisher_client_state.Players = {}

        # Добавляем текущего клиента (плеера) в Players
        if client_id not in publisher_client_state.Players:
            publisher_client_state.Players[client_id] = client_state  # Сохраняем объект ClientState
            self.logger.info("Player added to Players: %s for stream: %s", client_id, client_state.app)

        # Проверка: если у публикующего клиента есть сохраненные метаданные потока.
        if publisher_client_state.metaDataPayload != None:
            self.logger.info("Sending metadata to client: %s", client_id)
            # Отправка метаданных публикующего клиента клиенту, который запрашивает воспроизведение.
            # Создание объекта AMFBytesIO для записи данных AMF (Action Message Format).
            output = amf.AMFBytesIO()

            # Инициализация AMF0 для записи в поток.
            amfWriter = amf.AMF0(output)

            # Запись команды 'onMetaData' в AMF-формате (указывается тип передаваемых данных).
            amfWriter.write('onMetaData')

            # Запись метаданных (например, ширина, высота, битрейт) публикующего клиента.
            amfWriter.write(publisher_client_state.metaData)

            # Логирование отправки метаданных
            self.logger.info("Sent metadata to client: %s", client_id)

            # Дополнительное логирование содержимого метаданных
            self.logger.debug("Metadata payload: %s", publisher_client_state.metaDataPayload)
            self.logger.debug("Metadata object: %s", publisher_client_state.metaData)

            # Перемещение указателя на начало потока для чтения данных.
            output.seek(0)

            # Чтение готового AMF-пакета в формате байтов.
            payload = output.read()

            # Получение идентификатора потока из заголовка пакета (StreamID) текущего запроса на воспроизведение.
            streamId = invoke['packet']['header']['stream_id']

            # Формирование заголовка RTMP для передачи данных метаданных.
            # RTMP_CHANNEL_DATA — канал данных, RTMP_TYPE_DATA — тип пакета (метаданные).
            packet_header = common.Header(RTMP_CHANNEL_DATA, 0, len(payload), RTMP_TYPE_DATA, streamId)

            # Создание RTMP-сообщения с заголовком и полезной нагрузкой (метаданными).
            response = common.Message(packet_header, payload)

            # Отправка RTMP-сообщения клиенту, запросившему воспроизведение.
            await self.writeMessage(client_id, response)

        self.logger.info("Client %s started playing stream: %s", client_id, client_state.app)

    async def handle_publish(self, client_id, invoke):
        # Извлечение состояния клиента (объекта ClientState), соответствующего заданному client_id.
        client_state = self.client_states[client_id]
        # self.logger.info(f"client_state PUBLISH: {client_state}")

        # Определение режима стрима (по умолчанию 'live'). Если в аргументах `invoke['args']` больше одного элемента, используется второй аргумент.
        # Режим может быть: 'live', 'record', 'append'.
        # client_state.stream_mode = 'live' if len(invoke['args']) < 2 else invoke['args'][1]  # live, record, append
        # TODO УПРОЩЕННАЯ ВЕРСИЯ ДЛЯ ДЕМО
        client_state.stream_mode = 'live'

        # Установка пути потока (обычно это ключ потока), берется из первого аргумента `invoke['args']`.
        client_state.streamPath = invoke['args'][0]

        # Получение идентификатора потока из заголовка пакета (StreamID).
        client_state.publishStreamId = int(invoke['packet']['header']['stream_id'])

        # На всякий случай
        # if client_state.app not in LiveUsers:
        #     LiveUsers[client_state.app] = {}

        # Проверяем, существует ли поток
        if client_state.app in LiveUsers and LiveUsers[client_state.app].get('client_id'):
            self.logger.warning("Stream already publishing!")
            await self.sendStatusMessage(client_id, client_state.publishStreamId, "error", "NetStream.Publish.BadName",
                                         "Stream already publishing")
            raise DisconnectClientException()

        # Формирование полного пути публикации, состоящего из имени приложения (app) и ключа потока (streamPath).
        # Удаляет все параметры (например, ?key=value) из пути.
        self.logger.info(f"app PUBLISH: {client_state.app}")
        client_state.publishStreamPath = "/" + client_state.app + "/" + client_state.streamPath.split("?")[0]

        # Проверка: если путь потока не задан или пустой, отправляется ошибка клиенту и соединение разрывается.
        if client_state.streamPath == None or client_state.streamPath == '':
            self.logger.warning("Stream key is empty!")  # Логирование предупреждения о пустом ключе потока.
            await self.sendStatusMessage(
                client_id, client_state.publishStreamId, "error",
                "NetStream.publish.Unauthorized", "Authorization required."
            )  # Отправка сообщения клиенту о том, что публикация не авторизована.
            raise DisconnectClientException()  # Исключение для разрыва соединения с клиентом.

        # Проверяем, существует ли поток
        if client_state.app in LiveUsers and LiveUsers[client_state.app].get('client_id'):
            self.logger.warning("Stream already publishing!")
            await self.sendStatusMessage(client_id, client_state.publishStreamId, "error", "NetStream.Publish.BadName",
                                         "Stream already publishing")
            raise DisconnectClientException()

        # Если режим стрима — 'live', выполняется проверка на существующий поток с тем же приложением (app).
        if client_state.stream_mode == 'live':
            # Если поток с таким приложением уже публикуется, отправляется ошибка клиенту и соединение разрывается.
            if LiveUsers.get(client_state.app) is not None:
                self.logger.info(f"LiveUsers: {LiveUsers}")
                self.logger.info(f"PlayerUsers: {PlayerUsers}")
                self.logger.info(f"Players: {client_state.Players}")
                self.logger.warning("Stream already publishing!")  # Логирование предупреждения о существующем потоке.
                await self.sendStatusMessage(
                    client_id, client_state.publishStreamId, "error",
                    "NetStream.Publish.BadName", "Stream already publishing"
                )  # Отправка клиенту сообщения об ошибке.
                raise DisconnectClientException()  # Исключение для разрыва соединения.

            # Добавление нового потока в глобальный словарь `LiveUsers`.
            # Сохраняются идентификатор клиента, режим стрима, путь стрима, ID стрима и приложение.
            LiveUsers[client_state.app] = {
                'client_id': client_id,
                'stream_mode': client_state.stream_mode,
                'stream_path': client_state.streamPath,
                'publish_stream_id': client_state.publishStreamId,
                'app': client_state.app,
            }
            self.logger.info("Stream published: %s", LiveUsers[client_state.app])

        # Логирование текущего состояния глобального словаря `LiveUsers` (всех активных потоков).
        self.logger.info(f"LiveUsers: {LiveUsers}")

        # Логирование информации о запросе публикации, включая режим, приложение, путь, полный путь публикации и идентификатор потока.
        self.logger.info(
            "Publish Request Mode: %s, App: %s, Path: %s, publishStreamPath: %s, StreamID: %s",
            client_state.stream_mode, client_state.app, client_state.streamPath,
            client_state.publishStreamPath, str(client_state.publishStreamId)
        )

        # Отправка клиенту статуса о начале публикации (успешное начало потока).
        await self.sendStatusMessage(
            client_id, client_state.publishStreamId, "status",
            "NetStream.Publish.Start", f"{client_state.streamPath} is now published."
        )

    async def sendStatusMessage(self, client_id, sid, level, code, description):
        response = common.Command(
            name='onStatus',
            id=sid,
            tm=self.relativeTime(client_id),
            args=[
                amf.Object(
                    level=level,
                    code=code,
                    description=description,
                    details=None)])

        message = response.toMessage()
        self.logger.debug("Sending onStatus response!")
        await self.writeMessage(client_id, message)

    async def response_createStream(self, client_id, invoke):
        client_state = self.client_states[client_id]
        client_state.streams = client_state.streams + 1
        response = common.Command(
            name='_result',
            id=invoke['id'],
            tm=self.relativeTime(client_id),
            type=common.Message.RPC,
            args=[client_state.streams])

        message = response.toMessage()
        self.logger.debug("Sending createStream response!")
        await self.writeMessage(client_id, message)

    async def handle_connect_command(self, client_id, invoke):
        client_state = self.client_states[client_id]
        if hasattr(invoke['cmdData'], 'app'):
            client_state.app = invoke['cmdData'].app

        if client_state.app == '':
            self.logger.warning("Empty 'app' attribute. Disconnecting client: %s", client_state.client_ip)
            raise DisconnectClientException()

        if hasattr(invoke['cmdData'], 'tcUrl'):
            client_state.tcUrl = invoke['cmdData'].tcUrl

        if hasattr(invoke['cmdData'], 'swfUrl'):
            client_state.swfUrl = invoke['cmdData'].swfUrl

        if hasattr(invoke['cmdData'], 'flashVer'):
            client_state.flashVer = invoke['cmdData'].flashVer

        if hasattr(invoke['cmdData'], 'objectEncoding'):
            client_state.objectEncoding = invoke['cmdData'].objectEncoding

        self.logger.info("App: %s, tcUrl: %s, swfUrl: %s, flashVer: %s", client_state.app, client_state.tcUrl,
                         client_state.swfUrl, client_state.flashVer)

        await self.send_window_ack(client_id, 5000000)
        await self.set_chunk_size(client_id, client_state.out_chunk_size)
        await self.set_peer_bandwidth(client_id, 5000000, 2)
        await self.respond_connect(client_id, invoke['id'])

    async def send(self, client_id, data):
        client_state = self.client_states[client_id]
        # Perform asynchronous sending operation
        # self.logger.info("Sending data: %s", data)
        client_state.writer.write(data)
        await client_state.writer.drain()

    async def send_window_ack(self, client_id, size):
        rtmp_buffer = bytes.fromhex("02000000000004050000000000000000")
        rtmp_buffer = bytearray(rtmp_buffer)
        rtmp_buffer[12:16] = size.to_bytes(4, byteorder='big')
        await self.send(client_id, rtmp_buffer)
        self.logger.debug("Set ack to %s", size)

    async def send_ack(self, client_id, size):
        rtmp_buffer = bytes.fromhex("02000000000004030000000000000000")
        rtmp_buffer = bytearray(rtmp_buffer)
        rtmp_buffer[12:16] = size.to_bytes(4, byteorder='big')
        await self.send(client_id, rtmp_buffer)
        self.logger.debug("Send ACK: %s", size)

    async def set_peer_bandwidth(self, client_id, size, bandwidth_type):
        rtmp_buffer = bytes.fromhex("0200000000000506000000000000000000")
        rtmp_buffer = bytearray(rtmp_buffer)
        rtmp_buffer[12:16] = size.to_bytes(4, byteorder='big')
        rtmp_buffer[16] = bandwidth_type
        await self.send(client_id, rtmp_buffer)
        self.logger.debug("Set bandwidth to %s", size)

    async def set_chunk_size(self, client_id, out_chunk_size):
        rtmp_buffer = bytearray.fromhex("02000000000004010000000000000000")
        struct.pack_into('>I', rtmp_buffer, 12, out_chunk_size)
        await self.send(client_id, bytes(rtmp_buffer))
        self.logger.debug("Set out chunk to %s", out_chunk_size)

    async def handle_bytes_read_report(self, client_id, payload):
        # bytes_read = int.from_bytes(payload, byteorder='big')
        # self.logger.debug("Bytes read: %d", bytes_read)
        # # send ACK
        # rtmpBuffer = bytearray.fromhex('02000000000004030000000000000000')
        # rtmpBuffer[12:16] = bytes_read.to_bytes(4, 'big')
        # await self.send(client_id, rtmpBuffer) 
        # Just Ignore!
        return False

    async def respond_connect(self, client_id, tid):
        client_state = self.client_states[client_id]
        response = common.Command()
        response.id, response.name, response.type = tid, '_result', common.Message.RPC

        arg = amf.Object(
            level='status',
            code='NetConnection.Connect.Success',
            description='Connection succeeded.',
            fmsVer='MasterStream/8,2',
            capabilities=31,
            objectEncoding=client_state.objectEncoding)

        response.setArg(arg)
        message = response.toMessage()
        self.logger.debug("Sending connect response!")
        await self.writeMessage(client_id, message)

    def add_start_code(self, data):
        # Добавляет стартовый код к NAL-единицам
        # self.logger.info(f"RUN ADD_START_CODE")
        if not data.startswith(b'\x00\x00\x01'):
            # self.logger.info(f"DATA: {type(data)}")
            # self.logger.info(f"DATA: {type(bytearray(b'\x00\x00\x00\x01' + data))}")
            # self.logger.info(f"DATA: {b'\x00\x00\x00\x01' + data}")
            return bytearray(b'\x00\x00\x01' + data)
        return data

    async def writeMessage(self, client_id, message):
        # Проверка наличия client_id в client_states
        if client_id not in self.client_states:
            self.logger.error(f"Client {client_id} not found in client_states!")
            return

        client_state = self.client_states[client_id]

        # Инициализация header
        header = client_state.lastWriteHeaders.get(message.streamId)
        if not header:
            if client_state.nextChannelId <= PROTOCOL_CHANNEL_ID:
                client_state.nextChannelId = PROTOCOL_CHANNEL_ID + 1
            header = common.Header(client_state.nextChannelId)
            client_state.nextChannelId += 1
            client_state.lastWriteHeaders[message.streamId] = header

        if message.type < message.AUDIO:
            header = common.Header(PROTOCOL_CHANNEL_ID)

        self.logger.info(f"INITIALIZED HEADER: {header}")

        # Формирование header данных
        if header.streamId != message.streamId or header.time == 0 or message.time <= header.time:
            header.streamId = message.streamId
            header.type = message.type
            header.size = message.size
            header.time = message.time
            header.delta = message.time
            control = common.Header.FULL
        elif header.size != message.size or header.type != message.type:
            header.type = message.type
            header.size = message.size
            header.time = message.time
            header.delta = message.time - header.time
            control = common.Header.MESSAGE
        else:
            header.time = message.time
            header.delta = message.time - header.time
            control = common.Header.TIME

        self.logger.info(f"Control type: {control}")

        hdr = common.Header(
            channel=header.channel,
            time=header.delta if control in (common.Header.MESSAGE, common.Header.TIME) else header.time,
            size=header.size,
            type=header.type,
            streamId=header.streamId
        )

        data = b''

        # TODO Упрощенная логика для отладки
        # try:
        #     hdr = common.Header(
        #         channel=header.channel,
        #         time=0,  # Подставьте корректные значения
        #         size=len(message.data),
        #         type=message.type,
        #         streamId=message.streamId
        #     )
        #
        #     # Формируем пакет
        #     data = hdr.toBytes(common.Header.FULL) + message.data
        #     await self.send(client_id, data)
        #     # self.logger.info("Message sent successfully!")
        # except Exception as e:
        #     self.logger.error(f"Failed to send message: {e}")

        try:
            while len(message.data) > 0:
                self.logger.debug("Adding header to data stream...")
                data += hdr.toBytes(control)
                count = min(client_state.out_chunk_size, len(message.data))
                data += message.data[:count]
                message.data = message.data[count:]
                control = common.Header.SEPARATOR

            self.logger.debug(f"Prepared data to send (first 50 bytes): {data[:50]}")
            await self.send(client_id, data)
            # self.logger.info("Message sent successfully!")
        except KeyError as e:
            self.logger.error(f"KeyError during send: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during send: {e}")

    async def handle_amf_data(self, client_id, rtmp_packet):
        client_state = self.client_states[client_id]
        offset = 1 if rtmp_packet['header']['type'] == RTMP_TYPE_FLEX_MESSAGE else 0
        payload = rtmp_packet['payload'][offset:rtmp_packet['header']['length']]
        amfReader = amf.AMF0(payload)
        inst = {}
        inst['type'] = rtmp_packet['header']['type']
        inst['time'] = rtmp_packet['header']['timestamp']
        inst['packet'] = rtmp_packet
        inst['cmd'] = amfReader.read()  # first field is command name
        if inst['cmd'] == '@setDataFrame':
            inst['type'] = amfReader.read()  # onMetaData
            self.logger.debug("AMF Data type: %s", inst['type'])
            if inst['type'] != 'onMetaData':
                return

            inst['dataObj'] = amfReader.read()  # third is obj data
            if (inst['dataObj'] != None):
                self.logger.debug("Command Data %s", inst['dataObj'])
        else:
            self.logger.warning("Unsupported RTMP_TYPE_DATA cmd, CMD: %s", inst['cmd'])

        client_state.metaDataPayload = payload
        client_state.metaData = inst['dataObj']
        client_state.audioSampleRate = int(inst['dataObj']['audiosamplerate'])
        client_state.audioChannels = 2 if inst['dataObj']['stereo'] else 1
        client_state.videoWidth = int(inst['dataObj']['width'])
        client_state.videoHeight = int(inst['dataObj']['height'])
        client_state.videoFps = int(inst['dataObj']['framerate'])
        client_state.Bitrate = int(inst['dataObj']['videodatarate'])
        # TODO: handle Meta Data!

    def parse_amf0_invoke_message(self, rtmp_packet):
        offset = 1 if rtmp_packet['header']['type'] == RTMP_TYPE_FLEX_MESSAGE else 0
        payload = rtmp_packet['payload'][offset:rtmp_packet['header']['length']]
        amfReader = amf.AMF0(payload)
        inst = {}
        inst['type'] = rtmp_packet['header']['type']
        inst['time'] = rtmp_packet['header']['timestamp']
        inst['packet'] = rtmp_packet

        try:
            inst['cmd'] = amfReader.read()  # first field is command name
            if rtmp_packet['header']['type'] == RTMP_TYPE_FLEX_MESSAGE or rtmp_packet['header'][
                'type'] == RTMP_TYPE_INVOKE:
                inst['id'] = amfReader.read()  # second field *may* be message id
                inst['cmdData'] = amfReader.read()  # third is command data
                if (inst['cmdData'] != None):
                    self.logger.debug("Command Data %s", vars(inst['cmdData']))
            else:
                inst['id'] = 0
            inst['args'] = []  # others are optional
            while True:
                inst['args'].append(amfReader.read())  # amfReader.read()
        except EOFError:
            pass

        self.logger.debug("Command %s", inst)
        return inst

    def relativeTime(self, client_id):
        return int(1000 * (time.time() - self.client_states[client_id]._time0))

    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)

        addr = server.sockets[0].getsockname()
        self.logger.info("RTMP server started on %s", addr)

        async with server:
            await server.serve_forever()


# Configure logging level and format
logging.basicConfig(level=LogLevel, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename="py_log.log", filemode="w")
rtmp_server = RTMPServer()
asyncio.run(rtmp_server.start_server())
