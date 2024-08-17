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
from multiprocessing import Pool, cpu_count

load_dotenv()


class TranscriptionEngine:
    def __init__(self):
        # Load the transcription model from whisper
        if torch.cuda.is_available():
            print("++CUDA is available")
        else:
            print("--CUDA is not available")
        self.transcriptionModel = whisper.load_model(
            "small.en", device="cuda" if torch.cuda.is_available() else "cpu"
        )

        # Setup weaviate cloud connection
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers=self.headers,
        )

        # Setup Redis Client connection
        self.redisClient = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0
        )

        try:
            if self.weaviateClient.is_ready():
                print("Successfully connected to Weaviate")
                if not self.weaviateClient.collections.exists("ReelsTranscript"):
                    self.create_schema()
                else:
                    print("Collection already exists")
            else:
                print("Weaviate is not ready")

        except Exception as e:
            print(f"Error connecting to Weaviate: {e}")
            self.weaviateClient.close()

    def transcribeAndStore(self, videoId):
        self.weaviateClient = weaviate.connect_to_wcs(
            cluster_url=os.getenv("WEAVIATE_CLUSTER_URL"),
            auth_credentials=weaviate.auth.AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
            headers=self.headers,
        )

        self.redisClient = redis.Redis(
            host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=0
        )

        try:
            if self.weaviateClient.is_ready():
                transcriptionResult = self.transcribe(videoId)
                videoData = self.getVideoData(videoId)
                self.add_to_weaviate(transcriptionResult, videoData)
                self.cleanup_files(videoId)
                print("Transcription and storage complete", videoId)
            else:
                print("Weaviate is not ready")

        except Exception as e:
            print(f"Error connecting to Weaviate, writing failed: {e}")
        finally:
            self.weaviateClient.close()

    def cleanup_files(self, videoId):
        """Cleanup temporary files after processing."""
        if os.path.exists(f"./dataset/audio_files/{videoId}.mp3"):
            os.remove(f"./dataset/audio_files/{videoId}.mp3")
        if os.path.exists(f"./dataset/unparsed_json/{videoId}-reel.json"):
            os.remove(f"./dataset/unparsed_json/{videoId}-reel.json")
        if os.path.exists(f"./dataset/unparsed_json/{videoId}-player.json"):
            os.remove(f"./dataset/unparsed_json/{videoId}-player.json")

    def getFilePath(self):
        videoId = self.redisClient.lpop("downloaded_youtube_shorts")
        if videoId:
            videoId = videoId.decode("utf-8")
        else:
            return None
        return videoId

    def create_schema(self):
        print("Creating schema")
        try:
            self.weaviateClient.collections.create(
                name="ShortsTranscript",
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
                        name="username",
                        data_type=wvc.config.DataType.TEXT,
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
                        data_type=wvc.config.DataType.TEXT,
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
    ):
        try:
            self.weaviateClient.collections.get("ShortsTranscript").data.insert(
                properties={
                    "media_id": video_data["media_id"],
                    "username": video_data["username"],
                    "likes_count": video_data["likes_count"],
                    "comments_count": video_data["comments_count"],
                    "audio_id": video_data["audio_id"],
                    "caption": video_data["caption"],
                    "transcript": transcription,
                    "description": video_data["description"],
                },
            )
        except Exception as e:
            print(f"Error adding to Weaviate: {e}")

    def transcribe(self, videoId):
        result = self.transcriptionModel.transcribe(f'./dataset/audio_files/{videoId}.mp3')
        return result["text"]

    def getVideoData(self, videoId):
        jsonFilePath = f"./dataset/unparsed_json/{videoId}-reel.json"
        with open(jsonFilePath, "r", encoding="utf-8") as file:
            data = json.load(file)

            media_id = videoId

            try: 
                username = data["engagementPanels"][1][
                    "engagementPanelSectionListRenderer"
                ]["content"]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "channelNavigationEndpoint"
                ][
                    "commandMetadata"
                ][
                    "webCommandMetadata"
                ][
                    "url"
                ]
            except: 
                username = None

            try: 
                likes_count = data["overlay"]["reelPlayerOverlayRenderer"]["likeButton"][
                    "likeButtonRenderer"
                ]["likeCount"]
            except: 
                likes_count = None

            try: 
                comments_count = data["overlay"]["reelPlayerOverlayRenderer"][
                    "viewCommentsButton"
                ]["buttonRenderer"]["text"]["simpleText"]

            except: 
                comments_count = None

            try: 
                audio_id = data["overlay"]["reelPlayerOverlayRenderer"]["soundMetadata"][
                    "reelSoundMetadataViewModel"
                ]["onTapCommand"]["innertubeCommand"]["commandMetadata"][
                    "webCommandMetadata"
                ][
                    "url"
                ]

            except: 
                audio_id = None 

            try: 
                caption = data["engagementPanels"][1]["engagementPanelSectionListRenderer"][
                    "content"
                ]["structuredDescriptionContentRenderer"]["items"][0][
                    "videoDescriptionHeaderRenderer"
                ][
                    "title"
                ][
                    "runs"
                ][
                    0
                ][
                    "text"
                ]
            
            except: 
                caption = None
            try: 
                description = data["engagementPanels"][1]["engagementPanelSectionListRenderer"]["content"]["structuredDescriptionContentRenderer"]["items"][1]["expandableVideoDescriptionBodyRenderer"]["descriptionBodyText"]["runs"][0]["text"]
            except:
                description = None

        return {
            "media_id": media_id,
            "username": username,
            "likes_count": likes_count,
            "comments_count": comments_count,
            "audio_id": audio_id,
            "caption": caption,
            "description": description,
        }

    def process_files(self):
        with Pool(processes=cpu_count()) as pool:
            while True:
                filePaths = [self.getFilePath() for _ in range(cpu_count())]
                filePaths = [fp for fp in filePaths if fp]
                if filePaths:
                    pool.map(self.transcribeAndStore, filePaths)
                else:
                    print("No files to transcribe")
                    time.sleep(5)


if __name__ == "__main__":
    engine = TranscriptionEngine()
    engine.process_files()
