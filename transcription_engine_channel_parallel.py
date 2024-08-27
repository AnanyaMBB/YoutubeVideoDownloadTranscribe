import redis
import requests
import whisper
import torch
import weaviate
import weaviate.classes as wvc

import os
import json
import time
from dotenv import load_dotenv
from multiprocessing import Pool
import boto3
from botocore.client import Config
import io


load_dotenv()

class TranscriptionEngine:
    def __init__(self, gpu_id):
        # Set the specific GPU for this process
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

        # Load the transcription model from Whisper
        if torch.cuda.is_available():
            print(f"++CUDA is available on GPU {gpu_id}")
        else:
            print(f"--CUDA is not available on GPU {gpu_id}")
        self.transcriptionModel = whisper.load_model(
            "small.en", device="cuda" if torch.cuda.is_available() else "cpu"
        )

        # Setup Weaviate cloud connection
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers=self.headers,
        )

        # Setup Redis Client connection
        self.redisClient = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), username=os.getenv('REDIS_USERNAME'), password=os.getenv('REDIS_PASSWORD'), db=0
        )

        session = boto3.session.Session()
        self.client = session.client('s3',
                                region_name=os.getenv("SPACES_REGION_NAME"),
                                endpoint_url=f"https://{os.getenv('SPACES_REGION_NAME')}.digitaloceanspaces.com",
                                aws_access_key_id=os.getenv("SPACES_ACCESS_KEY"),
                                aws_secret_access_key=os.getenv("SPACES_SECRET_KEY"))

        try:
            # If Weaviate is properly connected, we proceed to other tasks
            if self.weaviateClient.is_ready():
                print("Successfully connected to Weaviate")
                if not self.weaviateClient.collections.exists("ChannelShortsTranscript"):
                    self.create_schema()
                else:
                    print("Collection already exists")
            else:
                print("Weaviate is not ready")

        except Exception as e:
            print(f"Error connecting to Weaviate: {e}")
            self.weaviateClient.close()

    def transcribeAndStore(self, videoId):
        max_retries = 1
        retry_delay = 1  # seconds
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Reconnect to Weaviate
                self.weaviateClient = weaviate.connect_to_wcs(
                    cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
                    auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
                    headers=self.headers,
                )

                # Setup Redis Client connection
                self.redisClient = redis.Redis(
                    host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), username=os.getenv('REDIS_USERNAME'), password=os.getenv('REDIS_PASSWORD'), db=0
                )

                # Check if Weaviate is ready
                if self.weaviateClient.is_ready():
                    transcriptionResult, language = self.transcribe(videoId) 
                    videoData = self.getVideoData(videoId)
                    self.add_to_weaviate(transcriptionResult, videoData, language)

                    # Clean up files
                    if os.path.exists(f"./dataset/audio_for_transcription/{videoId}.mp3"):
                        os.remove(f"./dataset/audio_for_transcription/{videoId}.mp3")
                    
                    print("Transcription and storage complete", videoId)
                    break  # Exit the retry loop after successful operation
                else:
                    print("Weaviate is not ready")
                    retry_count += 1
                    time.sleep(retry_delay)

            except Exception as e:
                print(f"Error during transcription and storage: {e}")
                retry_count += 1
                time.sleep(retry_delay)

            finally:
                self.weaviateClient.close()

        if retry_count >= max_retries:
            print(f"Failed to transcribe and store after {max_retries} retries.")

    def getFilePath(self):
        videoId, subscriberCount = self.redisClient.zrevrange(
                "channel_downloaded", 0, 0, withscores=True
            )[0]
        self.redisClient.zrem("channel_downloaded", videoId)

        if videoId:
            videoId = videoId.decode("utf-8")
        else:
            return None
        return videoId

    def create_schema(self):
        print("Creating schema")
        try:
            self.weaviateClient.collections.create(
                name="ChannelShortsTranscript",
                vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(
                    model="text-embedding-3-large", dimensions=1024
                ),
                generative_config=wvc.config.Configure.Generative.openai(),
                properties=[
                    wvc.config.Property(
                        name="media_id",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="channel_id",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="username",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="subscriber_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="views_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="likes_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="comments_count",
                        data_type=wvc.config.DataType.INT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="audio_id",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                    wvc.config.Property(
                        name="caption",
                        data_type=wvc.config.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.LOWERCASE,
                    ),
                    wvc.config.Property(
                        name="description",
                        data_type=wvc.config.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.LOWERCASE,
                    ),
                    wvc.config.Property(
                        name="transcript",
                        data_type=wvc.config.DataType.TEXT,
                        tokenization=wvc.config.Tokenization.LOWERCASE,
                    ),
                    wvc.config.Property(
                        name="language",
                        data_type=wvc.config.DataType.TEXT,
                        vectorize_property_name=False,
                        skip_vectorization=True,
                        index_filterable=True,
                    ),
                ],
                vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                    distance_metric=wvc.config.VectorDistances.COSINE,
                    quantizer=wvc.config.Configure.VectorIndex.Quantizer.bq(),
                ),
                inverted_index_config=wvc.config.Configure.inverted_index(
                    index_null_state=True,
                    index_property_length=True,
                    index_timestamps=True,
                ),
            )
        except Exception as e:
            print(f"Error creating schema: {e}")

    def add_to_weaviate(
        self,
        transcription,
        video_data,
        language="en",
    ):
        print("VIDEO DATA IN WEAVIATE", video_data)
        print("COMMENT COUNT: ", video_data["comment_count"])
        try:
            self.weaviateClient.collections.get("ChannelShortsTranscript").data.insert(
                properties={
                    "media_id": video_data["id"],
                    "channel_id": video_data["channel_id"],
                    "username": video_data.get("username", None),
                    "subscriber_count": int(video_data["subscriber_count"]),
                    "views_count": int(video_data["view_count"]),
                    "likes_count": video_data["like_count"],
                    "comments_count": int(video_data["comment_count"]),
                    "audio_id": video_data.get("audio_id", None),
                    "caption": video_data["title"],
                    "transcript": transcription,
                    "description": video_data["description"],
                    "language": language,
                },
            )
        except Exception as e:
            print(f"Error adding to Weaviate: {e}")

    def transcribe(self, videoId): 
        self.client.download_file(os.getenv("SPACES_SPACE_NAME"), f"youtube_files/dataset/channel_shorts/{videoId}.mp3", f"./dataset/audio_for_transcription/{videoId}.mp3")
        result = self.transcriptionModel.transcribe(f'./dataset/audio_for_transcription/{videoId}.mp3')
        return (result["text"], result["language"])

    def getVideoData(self, videoId):
        file = self.readFileFromSpace(f"youtube_files/dataset/channel_shorts_json/{videoId}.json")
        data = json.loads(file)

        print("Video data retrieved")
        print(data)
        return data
    
    def readFileFromSpace(self, space_file_name):
        try: 
            response = self.client.get_object(Bucket=os.getenv("SPACES_SPACE_NAME"), Key=space_file_name)
            file_content = response['Body'].read().decode('utf-8')
            print("FINISHED GRABBING FILE")
            return file_content
        except Exception as e:
            print(f"An error occurred reading from spaces: {e}")

    def readAudioFileFromSpace(self, space_file_name):
        try: 
            print("TRYING TO READ FILE")
            response = self.client.get_object(Bucket=os.getenv("SPACES_SPACE_NAME"), Key=space_file_name)
            file_content = response['Body'].read()
            return file_content
        except Exception as e:
            print(f"An error occurred reading from spaces: {e}")


def run_on_gpu(gpu_id):
    engine = TranscriptionEngine(gpu_id)
    while True:
        filePath = engine.getFilePath()
        print(f"GPU {gpu_id} processing file: {filePath}")
        if filePath:
            engine.transcribeAndStore(filePath)
        else:
            print(f"No files to transcribe on GPU {gpu_id}")
            time.sleep(5)


if __name__ == "__main__":
    gpu_ids = [0, 1, 2, 3]  # Assuming you have 4 GPUs

    # Using a pool of workers to parallelize across GPUs
    with Pool(len(gpu_ids)) as p:
        p.map(run_on_gpu, gpu_ids)
