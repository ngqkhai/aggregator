import asyncio
import json
import logging
import aio_pika
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractQueue
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from app.config import settings

# Load environment variables
load_dotenv()

# Logger setup with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("aggregator.broker")

class AggregatorMessageBroker:
    def __init__(self):
        self.connection: AbstractConnection = None
        self.channel: AbstractChannel = None
        self.voice_queue: AbstractQueue = None
        self.image_queue: AbstractQueue = None
        self.events_exchange: aio_pika.Exchange = None
        self.events_queue: AbstractQueue = None
        self.db_client: AsyncIOMotorClient = None
        self.collection = None
        # track pending jobs: {script_id: {script, voice, image}}
        self.pending: dict = {}
        logger.info("AggregatorMessageBroker initialized")

    async def connect(self):
        
        logger.info(f"Connecting to RabbitMQ at {settings.RABBITMQ_URL.split('@')[-1]}")
        try:
            # Connect to RabbitMQ
            self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            logger.info("RabbitMQ connection established")
            
            self.channel = await self.connection.channel()
            logger.info("RabbitMQ channel opened")

            # Declare exchanges for results
            logger.info(f"Declaring voice results exchange: {settings.VOICE_RESULTS_EXCHANGE}")
            voice_ex = await self.channel.declare_exchange(
                settings.VOICE_RESULTS_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            logger.info(f"Declaring image results exchange: {settings.IMAGE_RESULTS_EXCHANGE}")
            image_ex = await self.channel.declare_exchange(
                settings.IMAGE_RESULTS_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            # Declare and bind voice results queue
            logger.info(f"Declaring voice results queue: {settings.VOICE_RESULTS_QUEUE}")
            self.voice_queue = await self.channel.declare_queue(
                settings.VOICE_RESULTS_QUEUE,
                durable=True
            )
            
            logger.info(f"Binding voice queue to exchange with routing key: {settings.VOICE_RESULTS_ROUTING_KEY}")
            await self.voice_queue.bind(
                voice_ex,
                routing_key=settings.VOICE_RESULTS_ROUTING_KEY
            )
            
            # Declare and bind image results queue
            logger.info(f"Declaring image results queue: {settings.IMAGE_RESULTS_QUEUE}")
            self.image_queue = await self.channel.declare_queue(
                settings.IMAGE_RESULTS_QUEUE,
                durable=True
            )
            
            logger.info(f"Binding image queue to exchange with routing key: {settings.IMAGE_RESULTS_ROUTING_KEY}")
            await self.image_queue.bind(
                image_ex,
                routing_key=settings.IMAGE_RESULTS_ROUTING_KEY
            )

            # Declare exchange for final script.ready events
            logger.info(f"Declaring script events exchange: {settings.SCRIPT_EVENTS_EXCHANGE}")
            self.events_exchange = await self.channel.declare_exchange(
                settings.SCRIPT_EVENTS_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            logger.info(f"Declaring script events queue: {settings.SCRIPT_EVENTS_QUEUE}")
            self.events_queue = await self.channel.declare_queue(
                settings.SCRIPT_EVENTS_QUEUE,
                durable=True
            )
            logger.info(f"Binding script events queue to exchange with routing key: {settings.SCRIPT_EVENTS_ROUTING_KEY}")
            await self.events_queue.bind(
                self.events_exchange,
                routing_key=settings.SCRIPT_EVENTS_ROUTING_KEY
            )

            logger.info("RabbitMQ connected and queues/exchanges declared successfully")

            # Connect to MongoDB
            logger.info(f"Connecting to MongoDB at {settings.MONGODB_URL.split('@')[-1]}")
            self.db_client = AsyncIOMotorClient(settings.MONGODB_URL)
            self.collection = self.db_client[settings.MONGODB_DB][settings.MONGODB_COLLECTION]
            
            # Verify MongoDB connection
            try:
                await self.db_client.admin.command('ping')
                logger.info("MongoDB connection successful - Connected to %s.%s", 
                           settings.MONGODB_DB, settings.MONGODB_COLLECTION)
            except Exception as e:
                logger.error(f"MongoDB connection verification failed: {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"Connection setup failed: {str(e)}", exc_info=True)
            raise

    async def start(self):
        logger.info("Starting message consumption")
        try:
            # Start consuming both result streams concurrently
            await asyncio.gather(
                self.consume_voice(),
                # self.consume_image()
            )
        except Exception as e:
            logger.error(f"Error in start method: {str(e)}", exc_info=True)
            raise

    async def consume_voice(self):
        logger.info(f"👂 Consuming voice results from queue: {settings.VOICE_RESULTS_QUEUE}")
        try:
            logger.info(f"Voice queue: {self.voice_queue}")
            async with self.voice_queue.iterator() as queue_iter:
                logger.info("Voice queue iterator established")
                message_count = 0
                async for message in queue_iter:
                    message_count += 1
                    logger.info(f"🎵 Received voice message #{message_count}")
                    async with message.process():
                        try:
                            body = message.body.decode()
                            logger.debug(f"Voice message body: {body[:100]}...")
                            data = json.loads(body)
                            script_id = data.get("script_id", "unknown")
                            logger.info(f"Processing voice result for script_id: {script_id}")
                            await self.handle_result("voice", data)
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error in voice message: {str(e)}")
                            logger.error(f"Raw message body: {message.body}")
                        except Exception as e:
                            logger.error(f"Error processing voice message: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in consume_voice method: {str(e)}", exc_info=True)
            # Try to reconnect after error
            logger.info("Attempting to reconnect voice consumer in 5 seconds...")
            await asyncio.sleep(5)
            await self.consume_voice()

    async def consume_image(self):
        logger.info(f"👂 Consuming image results from queue: {settings.IMAGE_RESULTS_QUEUE}")
        try:
            async with self.image_queue.iterator() as queue_iter:
                logger.info("Image queue iterator established")
                message_count = 0
                async for message in queue_iter:
                    message_count += 1
                    logger.info(f"🖼️ Received image message #{message_count}")
                    async with message.process():
                        try:
                            body = message.body.decode()
                            logger.debug(f"Image message body: {body[:100]}...")
                            data = json.loads(body)
                            script_id = data.get("script_id", "unknown")
                            logger.info(f"Processing image result for script_id: {script_id}")
                            await self.handle_result("image", data)
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error in image message: {str(e)}")
                            logger.error(f"Raw message body: {message.body}")
                        except Exception as e:
                            logger.error(f"Error processing image message: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in consume_image method: {str(e)}", exc_info=True)
            # Try to reconnect after error
            logger.info("Attempting to reconnect image consumer in 5 seconds...")
            await asyncio.sleep(5)
            await self.consume_image()

    async def handle_result(self, result_type: str, data: dict):
        script_id = data.get("script_id")
        if not script_id:
            logger.warning(f"⚠️ Received {result_type} result without script_id: {data}")
            return
            
        logger.info(f"📥 Handling {result_type} result for script_id: {script_id}")
        
        # Initialize the pending job if needed
        if script_id not in self.pending:
            logger.info(f"Creating new pending job for script_id: {script_id}")
            
            # Fetch the original script from MongoDB
            try:
                script_doc = await self.collection.find_one({"_id": script_id})
                if not script_doc:
                    logger.error(f"❌ Script document not found in MongoDB for id: {script_id}")
                    return
                logger.info(f"Retrieved script document from MongoDB for id: {script_id}")
                
                self.pending[script_id] = {"script": script_doc}
                logger.info(f"Pending jobs count: {len(self.pending)}")
            except Exception as e:
                logger.error(f"MongoDB query error for script_id {script_id}: {str(e)}", exc_info=True)
                return
        
        # Store the result
        self.pending[script_id][result_type] = data
        logger.info(f"Added {result_type} result to pending job {script_id}")
        
        # Log the current state of this job
        job = self.pending[script_id]
        available_components = [k for k in job.keys()]
        logger.info(f"Job {script_id} has components: {available_components}")
        
        # If both voice and image are available, publish ready event
        # if "voice" in job and "image" in job:
        if "voice" in job:
            logger.info(f"✅ All required components available for script_id: {script_id}")
            await self.publish_ready(script_id)
        else:
            logger.info(f"⏳ Waiting for more components for script_id: {script_id}")

    async def publish_ready(self, script_id: str):
        logger.info(f"📤 Publishing ready event for script_id: {script_id}")
        try:
            job = self.pending.pop(script_id)
            logger.info(f"Removed job from pending queue. Remaining jobs: {len(self.pending)}")
            
            ready_payload = {
                "script": job.get("script"),
                "voice": job.get("voice"),
                # "image": job.get("image")
            }
            
            # Log payload size and content summary
            script_content = job.get("script", {}).get("content", "")
            voice_url = job.get("voice", {}).get("cloudinary_url", "")
            
            logger.info(f"Ready payload summary - Script ID: {script_id}")
            logger.info(f"Script content length: {len(script_content) if script_content else 0} chars")
            logger.info(f"Voice URL: {voice_url}")
            
            # Convert to JSON string first for better error handling
            try:
                json_body = json.dumps(ready_payload, default=str)
                logger.info(f"JSON serialization successful. Payload size: {len(json_body)} bytes")
            except TypeError as e:
                logger.error(f"JSON serialization error: {str(e)}")
                # Print problematic fields
                for k, v in ready_payload.items():
                    try:
                        json.dumps({k: v}, default=str)
                    except TypeError:
                        logger.error(f"Field '{k}' has serialization issues")
                raise
            
            message = aio_pika.Message(
                body=json_body.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )
            
            await self.events_exchange.publish(
                message,
                routing_key=settings.SCRIPT_EVENTS_ROUTING_KEY
            )
            
            logger.info(f"✨ Published script.ready event for script_id: {script_id} with routing key: {settings.SCRIPT_EVENTS_ROUTING_KEY}")
            
        except Exception as e:
            logger.error(f"Error publishing ready event for script_id {script_id}: {str(e)}", exc_info=True)
            # Put the job back in pending to avoid data loss
            if 'job' in locals():
                self.pending[script_id] = job
                logger.info(f"Job put back in pending queue due to error")

    async def close(self):
        logger.info("Shutting down aggregator...")
        # Close MongoDB connection
        if self.db_client:
            self.db_client.close()
            logger.info("MongoDB connection closed")
        
        # Close RabbitMQ connection
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed")
        
        # Report any pending jobs that weren't completed
        if self.pending:
            logger.warning(f"⚠️ {len(self.pending)} jobs still pending at shutdown")
            for script_id, job in self.pending.items():
                components = [k for k in job.keys()]
                logger.warning(f"Pending job {script_id} has components: {components}")
                
        logger.info("Aggregator shutdown complete")