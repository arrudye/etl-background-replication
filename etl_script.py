import os
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
import pymongo
from pymongo import UpdateOne
import schedule

load_dotenv()

LAST_SYNC_DIR = '/app/last_sync'
os.makedirs(LAST_SYNC_DIR, exist_ok=True)
LAST_SYNC_FILE = os.path.join(LAST_SYNC_DIR, 'last_sync.txt')

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'dbname': os.getenv('POSTGRES_DB', 'shop'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST', 'mongodb'),
    'port': int(os.getenv('MONGO_PORT', '27017')),
    'db': os.getenv('MONGO_DB', 'shop_replica'),
    'collection': os.getenv('MONGO_COLLECTION', 'customers')
}

SYNC_INTERVAL = int(os.getenv('SYNC_INTERVAL', '60'))
MAX_RETRIES = 300
RETRY_DELAY = 2

# читает время последней успешной синх-ии из файла, иначе берет старую дату
def read_last_sync():
    try:
        with open(LAST_SYNC_FILE, 'r') as f:
            last_sync = f.read().strip()
            if last_sync:
                logger.info(f"Последняя синхронизация: {last_sync}")
                return last_sync
    except FileNotFoundError:
        logger.info("Файл последней синхронизации не найден. Будет выполнена полная синхронизация.")
    return '1970-01-01 00:00:00'

# сохраняет текущее время как время последней синх-ии
def save_last_sync():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LAST_SYNC_FILE, 'w') as f:
        f.write(current_time)
    logger.info(f"Время синхронизации сохранено: {current_time}")
    return current_time

# подключаемся к бд
def get_postgres_connection():
    retries = 0
    last_exception = None
    
    while retries < MAX_RETRIES:
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.set_client_encoding('UTF8')
            logger.info("Успешное подключение к PostgreSQL")
            return conn
        except OperationalError as e:
            last_exception = e
            retries += 1
            if retries < MAX_RETRIES:
                logger.warning(f"Попытка подключения к PostgreSQL {retries}/{MAX_RETRIES} не удалась. Повтор через {RETRY_DELAY} сек...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Не удалось подключиться к PostgreSQL после {MAX_RETRIES} попыток")
    
    raise last_exception

def get_mongo_connection():
    retries = 0
    last_exception = None

    while retries < MAX_RETRIES:
        try:
            client = pymongo.MongoClient(
                host=MONGO_CONFIG['host'], 
                port=MONGO_CONFIG['port'],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            client.admin.command('ping')
            logger.info("Успешное подключение к MongoDB")
            collection = client[MONGO_CONFIG['db']][MONGO_CONFIG['collection']]
            return collection
        except Exception as e:
            last_exception = e
            retries += 1
            if retries < MAX_RETRIES:
                logger.warning(f"Попытка подключения к MongoDB {retries}/{MAX_RETRIES} не удалась: {e}. Повтор через {RETRY_DELAY} сек...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Не удалось подключиться к MongoDB после {MAX_RETRIES} попыток")
    
    raise last_exception

# непосредственно ETL-репликация: 
# Extract (извлекает новые данные из pg), 
# Transform (преобразует их из одной структуры в другую),
# Load (загружает данные в mongo)
def replicate():
    logger.info("Начало цикла синхронизации")
    try:
        last_sync = read_last_sync()
        
        pg_conn = get_postgres_connection()
        mongo_collection = get_mongo_connection()
        
        # extract
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, email, created_at, deleted_at
                FROM customers
                WHERE created_at > %s OR (deleted_at > %s AND deleted_at IS NOT NULL)
            """, (last_sync, last_sync))
            new_customers = cursor.fetchall()
            logger.info(f"Найдено новых/измененных покупателей: {len(new_customers)}")
            
            # продукты вложены в заказы
            cursor.execute("""
                SELECT 
                    o.id as order_id,
                    o.customer_id,
                    o.status,
                    o.created_at as order_created,
                    o.updated_at,
                    o.deleted_at as order_deleted,
                    c.id as customer_id,
                    c.name as customer_name,
                    c.email as customer_email,
                    json_agg(
                        json_build_object(
                            'product_id', p.id,
                            'product_name', p.name,
                            'quantity', oi.quantity,
                            'price_at_time', oi.price_at_time,
                            'deleted_at', oi.deleted_at
                        ) ORDER BY oi.id
                    ) FILTER (WHERE oi.id IS NOT NULL) as items
                FROM orders o
                JOIN customers c ON c.id = o.customer_id
                LEFT JOIN order_items oi ON oi.order_id = o.id
                LEFT JOIN products p ON p.id = oi.product_id
                WHERE o.updated_at > %s OR (o.deleted_at > %s AND o.deleted_at IS NOT NULL)
                GROUP BY o.id, o.customer_id, o.status, o.created_at, o.updated_at, o.deleted_at, 
                         c.id, c.name, c.email
                ORDER BY o.customer_id, o.created_at
            """, (last_sync, last_sync))
            new_orders = cursor.fetchall()
            logger.info(f"Найдено новых/измененных заказов: {len(new_orders)}")

        # transform
        operations = []
        customer_updates = {}

        # сначала собираем покупателей
        for customer in new_customers:
            customer_id, name, email, created_at, deleted_at = customer
            
            customer_updates[customer_id] = {
                '_id': customer_id,
                'name': name,
                'email': email,
                'created_at': created_at,
                'deleted_at': deleted_at,
                'synced_at': datetime.now()
            }

        # группируем заказы по покупателям
        orders_by_customer = {}
        for order in new_orders:
            (order_id, customer_id, status, order_created, updated_at, order_deleted,
             _, _, _, items) = order
            
            if customer_id not in orders_by_customer:
                orders_by_customer[customer_id] = []
            
            order_items = []
            if items:
                for item in items:
                    if item['product_id']:
                        order_items.append({
                            'product_id': item['product_id'],
                            'product_name': item['product_name'],
                            'quantity': item['quantity'],
                            'price_at_time': float(item['price_at_time']),
                            'deleted_at': item['deleted_at']
                        })
            
            order_doc = {
                'order_id': order_id,
                'status': status,
                'placed_at': order_created,
                'updated_at': updated_at,
                'deleted_at': order_deleted,
                'items': order_items
            }
            orders_by_customer[customer_id].append(order_doc)
        
        # добавляем их к покупателям
        for customer_id, orders_list in orders_by_customer.items():
            if customer_id in customer_updates:
                customer_updates[customer_id]['orders'] = orders_list
            else:
                customer_updates[customer_id] = {
                    '_id': customer_id,
                    'orders': orders_list,
                    'synced_at': datetime.now()
                }

        for customer_id, update_data in customer_updates.items():
            operations.append(
                UpdateOne(
                    {'_id': customer_id},
                    {'$set': update_data},
                    #базовая идемпотентность: если документ существует, то он обновитчя, иначе вставляет новый документ
                    upsert=True
                )
            )
        
        # load
        if operations:
            result = mongo_collection.bulk_write(operations, ordered=False)
            logger.info(f"Вставлено/обновлено документов в MongoDB: {result.upserted_count + result.modified_count}")
        else:
            logger.info("Нет данных для синхронизации")
        
        _ = save_last_sync()
        
        pg_conn.close()
        
        logger.info("Цикл синхронизации успешно завершен")
        
    except Exception as e:
        logger.error(f"Ошибка в цикле синхронизации: {e}", exc_info=True)
        time.sleep(10)

if __name__ == "__main__":
    logger.info("Запуск ETL-сервиса. Ожидание готовности PostgreSQL...")
    time.sleep(5)

    replicate()
    
    # планирует периодические запуски
    schedule.every(SYNC_INTERVAL).seconds.do(replicate)
    
    while True:
        schedule.run_pending()
        time.sleep(1)