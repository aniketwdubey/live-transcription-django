from channels.generic.websocket import AsyncWebsocketConsumer
from dotenv import load_dotenv
from deepgram import Deepgram
from typing import Dict

import os

load_dotenv()

from google.cloud import storage
import tempfile

class TranscriptConsumer(AsyncWebsocketConsumer):
    dg_client = Deepgram(os.getenv('DEEPGRAM_API_KEY'))
    storage_client = storage.Client.from_service_account_json('/Users/aniketdubey/Desktop/live-transcription-django/genai-pilot-37479-cd3e12cdd58a.json')
    bucket_name = 'real-time-transcriber'

    async def connect(self):
        self.audio_data = b''
        self.transcription = ''
        await self.connect_to_deepgram()
        await self.accept()

    async def connect_to_deepgram(self):
        try:
            self.socket = await self.dg_client.transcription.live({'punctuate': True, 'interim_results': False})
            self.socket.registerHandler(self.socket.event.CLOSE, lambda c: print(f'Connection closed with code {c}.'))
            self.socket.registerHandler(self.socket.event.TRANSCRIPT_RECEIVED, self.get_transcript)
        except Exception as e:
            raise Exception(f'Could not open socket: {e}')

    async def receive(self, bytes_data):
        self.audio_data += bytes_data
        self.socket.send(bytes_data)

    async def disconnect(self, close_code):
        # Save audio data to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_audio_file:
            temp_audio_file.write(self.audio_data)
            temp_audio_file_path = temp_audio_file.name

        # Upload audio file to GCS
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob('audio_file.webm')
        blob.upload_from_filename(temp_audio_file_path)

        # Save transcription to GCS
        transcript_blob = bucket.blob('transcription.txt')
        transcript_blob.upload_from_string(self.transcription)

        # Clean up
        os.remove(temp_audio_file_path)

    async def get_transcript(self, data: Dict) -> None:
        if 'channel' in data:
            transcript = data['channel']['alternatives'][0]['transcript']
            if transcript:
                self.transcription += transcript + ' '
                await self.send(transcript)
